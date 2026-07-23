"""Производство: проекты без финансовых данных (доступ для менеджеров user)."""
from datetime import datetime

from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from flask_login import login_required, current_user
from sqlalchemy import func
import io

from app.models import (
    db, Plant, Size, Field, Project, ProjectItem, ProjectPottingLog, ProjectPottingDayMeta,
)
from app.utils import msk_now, msk_today, log_action, timesheet_workers_count_on_date, natural_key
from app.finance import (
    _project_potting_context,
    _project_potting_analytics,
    _project_potting_day_workers_context,
    _potting_day_qty_for_form,
    _save_potting_day_workers,
    apply_daily_potting_save,
    apply_potting_log_edit,
    delete_potting_log_row,
    apply_project_potting_recount,
    apply_project_item_potting_receipt,
    apply_project_settings_from_form,
    _get_project_potting_recount_doc,
)

bp = Blueprint('production', __name__, url_prefix='/production')

PRODUCTION_ROLES = frozenset({'user', 'admin', 'executive'})


def _check_production_access():
    if current_user.role not in PRODUCTION_ROLES:
        return redirect(url_for('main.index'))
    return None


def _can_edit_production(project):
    return (
        current_user.role in PRODUCTION_ROLES
        and project.status == 'active'
    )


@bp.route('/projects', methods=['GET', 'POST'])
@login_required
def projects_list():
    denied = _check_production_access()
    if denied:
        return denied

    if request.method == 'POST':
        if current_user.role not in ('admin', 'executive'):
            flash('Недостаточно прав для создания проекта', 'warning')
            return redirect(url_for('production.projects_list'))
        name = request.form.get('name')
        desc = request.form.get('description')
        if name:
            new_proj = Project(name=name, description=desc, status='active')
            db.session.add(new_proj)
            db.session.commit()
            flash(f'Проект "{name}" создан')
            log_action(f"Создал проект {name}")
        return redirect(url_for('production.projects_list'))

    show_closed = request.args.get('show_closed') == '1'
    query = Project.query
    if not show_closed:
        query = query.filter_by(status='active')
    projects_db = query.order_by(Project.created_at.desc()).all()

    projects_data = []
    for p in projects_db:
        items = list(p.items)
        item_ids = [i.id for i in items]
        potted_total = 0
        if item_ids:
            potted_total = int(db.session.query(
                func.sum(ProjectPottingLog.quantity),
            ).filter(
                ProjectPottingLog.project_item_id.in_(item_ids),
            ).scalar() or 0)
        supply_total = sum(i.quantity or 0 for i in items)
        projects_data.append({
            'id': p.id,
            'name': p.name,
            'description': p.description,
            'status': p.status,
            'items_count': len(items),
            'supply_total': supply_total,
            'potted_total': potted_total,
        })

    return render_template(
        'finance/projects_list.html',
        production_view=True,
        active_tab='list',
        projects=projects_data,
        report_data=[],
        show_closed=show_closed,
        selected_year=msk_now().year,
        current_year=msk_now().year,
    )


@bp.route('/project/<int:project_id>/potting-recount', methods=['POST'])
@login_required
def project_potting_recount(project_id):
    denied = _check_production_access()
    if denied:
        return denied
    if current_user.role not in ('admin', 'executive'):
        flash('Недостаточно прав', 'warning')
        return redirect(url_for('production.project_detail', project_id=project_id))

    project = Project.query.get_or_404(project_id)
    result = apply_project_potting_recount(project, current_user.id)
    flash(result['message'], 'success' if result['ok'] else 'warning')
    return redirect(url_for('production.project_detail', project_id=project.id))


@bp.route('/project/<int:project_id>', methods=['GET', 'POST'])
@login_required
def project_detail(project_id):
    denied = _check_production_access()
    if denied:
        return denied

    project = Project.query.get_or_404(project_id)
    redirect_potting_date = None
    can_edit = _can_edit_production(project)

    if request.method == 'POST':
        if 'seedling_action' in request.form:
            from app.seedlings import handle_seedling_project_form
            ok, msg, category = handle_seedling_project_form(project, request.form, current_user.id)
            flash(msg, category)
            if ok:
                log_action(f'Саженцы/проект #{project.id}: {msg}')
            return redirect(url_for('production.project_detail', project_id=project.id))

        if not can_edit and any(k in request.form for k in (
            'add_item', 'bulk_add_items', 'delete_item',
            'save_daily_potting', 'delete_potting_log', 'edit_potting_log',
        )):
            flash('Проект закрыт — редактирование недоступно', 'warning')
            return redirect(url_for('production.project_detail', project_id=project.id))

        if 'update_project' in request.form:
            if current_user.role not in ('admin', 'executive'):
                flash('Недостаточно прав', 'warning')
            else:
                ok, msg, category = apply_project_settings_from_form(project, request.form)
                flash(msg, category)

        elif 'add_item' in request.form:
            plant_id = request.form.get('plant_id')
            size_id = request.form.get('size_id')
            qty = int(request.form.get('quantity') or 0)
            db.session.add(ProjectItem(
                project_id=project.id, plant_id=plant_id, size_id=size_id, quantity=qty,
            ))
            db.session.commit()
            flash('Позиция добавлена')

        elif 'bulk_add_items' in request.form:
            plant_ids = request.form.getlist('bulk_plant_id[]')
            size_ids = request.form.getlist('bulk_size_id[]')
            qtys = request.form.getlist('bulk_quantity[]')
            added = skipped = 0
            for i in range(len(plant_ids)):
                pid = (plant_ids[i] or '').strip()
                sid = (size_ids[i] or '').strip()
                q_raw = (qtys[i] if i < len(qtys) else '').strip()
                if not pid or not sid or not q_raw:
                    skipped += 1
                    continue
                try:
                    pid_i = int(pid)
                    sid_i = int(sid)
                    q_i = int(q_raw)
                except (TypeError, ValueError):
                    skipped += 1
                    continue
                if q_i <= 0:
                    skipped += 1
                    continue
                db.session.add(ProjectItem(
                    project_id=project.id, plant_id=pid_i, size_id=sid_i, quantity=q_i,
                ))
                added += 1
            if added:
                db.session.commit()
                flash(f'Добавлено позиций: {added}' + (f' (пропущено {skipped})' if skipped else ''))
                log_action(f'Массовое добавление в проект {project.id}: {added} строк')
            else:
                flash('Не добавлено ни одной строки. Проверьте заполнение.', 'warning')

        elif 'delete_item' in request.form:
            if current_user.role == 'user':
                flash('Недостаточно прав для удаления позиций поставки', 'warning')
            else:
                ProjectItem.query.filter_by(
                    id=request.form.get('item_id'), project_id=project.id,
                ).delete()
                db.session.commit()
                flash('Позиция удалена')

        elif 'save_daily_potting' in request.form:
            d_raw = (request.form.get('potting_date') or '').strip()
            try:
                log_date = datetime.strptime(d_raw, '%Y-%m-%d').date()
            except (TypeError, ValueError):
                log_date = msk_today()

            saved, cleared = apply_daily_potting_save(
                project,
                log_date,
                request.form.getlist('potting_item_id[]'),
                request.form.getlist('potting_qty[]'),
                current_user.id,
            )
            if current_user.role == 'user':
                ts_workers = timesheet_workers_count_on_date(log_date)
                _save_potting_day_workers(project, log_date, str(ts_workers))
            else:
                _save_potting_day_workers(project, log_date, request.form.get('potting_workers'))
            db.session.commit()
            if saved or cleared:
                flash(
                    f'Посадка в горшки за {log_date.strftime("%d.%m.%Y")}: '
                    f'добавлено позиций — {saved}'
                    + (f', очищено — {cleared}' if cleared else '')
                )
                log_action(f'Посадка в горшки, проект {project.id}, дата {log_date}')
                redirect_potting_date = log_date.isoformat()
            elif current_user.role != 'user' and request.form.get('potting_workers', '').strip() != '':
                flash(f'Число работников за {log_date.strftime("%d.%m.%Y")} сохранено', 'success')
                redirect_potting_date = log_date.isoformat()
            else:
                flash('Нет данных для сохранения', 'warning')

        elif 'edit_potting_log' in request.form:
            if current_user.role not in ('admin', 'executive'):
                flash('Недостаточно прав', 'warning')
            else:
                ok, msg, category = apply_potting_log_edit(
                    project,
                    request.form.get('potting_log_id'),
                    request.form.get('potting_log_qty'),
                )
                flash(msg, category)

        elif 'delete_potting_log' in request.form:
            if current_user.role not in ('admin', 'executive'):
                flash('Недостаточно прав', 'warning')
            else:
                ok, msg, category = delete_potting_log_row(
                    project, request.form.get('potting_log_id'),
                )
                flash(msg, category)

        elif 'receipt_item_stock' in request.form:
            if current_user.role != 'admin':
                flash('Недостаточно прав', 'warning')
            else:
                result = apply_project_item_potting_receipt(
                    project, request.form.get('item_id'), current_user.id,
                )
                flash(result['message'], 'success' if result['ok'] else 'warning')

        else:
            flash('Действие недоступно в режиме производства', 'warning')

        if redirect_potting_date:
            return redirect(url_for(
                'production.project_detail',
                project_id=project.id,
                potting_date=redirect_potting_date,
                potting_day_cleared=1,
            ))
        return redirect(url_for('production.project_detail', project_id=project.id))

    potting_ctx = _project_potting_context(project)
    potting_analytics = _project_potting_analytics(project)
    from app.seedlings import seedling_context_for_project
    seedling_ctx = seedling_context_for_project(project)
    view_date_raw = request.args.get('potting_date', '').strip()
    try:
        view_date = datetime.strptime(view_date_raw, '%Y-%m-%d').date() if view_date_raw else msk_today()
    except ValueError:
        view_date = msk_today()
    potting_day_qty = _potting_day_qty_for_form(
        project, view_date,
        skip_load=request.args.get('potting_day_cleared') == '1',
    )

    return render_template(
        'finance/project_detail.html',
        production_view=True,
        can_edit_production=can_edit,
        project=project,
        stats=None,
        budget_comparison=[],
        all_orders=[],
        plants=sorted(Plant.query.all(), key=lambda x: x.name),
        sizes=Size.query.all(),
        fields=sorted(Field.query.all(), key=natural_key),
        potting_recount_doc=_get_project_potting_recount_doc(project),
        potting_view_date=view_date.isoformat(),
        potting_day_qty=potting_day_qty,
        **_project_potting_day_workers_context(project, view_date),
        **potting_ctx,
        **potting_analytics,
        **seedling_ctx,
    )


@bp.route('/project/<int:project_id>/seedling_dieback.xlsx')
@login_required
def seedling_dieback_export(project_id):
    denied = _check_production_access()
    if denied:
        return denied
    project = Project.query.get_or_404(project_id)
    from app.seedlings import export_dieback_workbook
    wb = export_dieback_workbook(project.id)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f"vypad_project_{project.id}.xlsx"
    return send_file(
        buf,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


@bp.route('/project/<int:project_id>/seedling_dieback_summary.xlsx')
@login_required
def seedling_dieback_summary_export(project_id):
    denied = _check_production_access()
    if denied:
        return denied
    project = Project.query.get_or_404(project_id)
    from app.seedlings import export_dieback_summary_workbook
    from app.utils import msk_now
    year = int(request.args.get('year') or msk_now().year)
    period_type = (request.args.get('period') or 'month').strip()
    try:
        period_n = int(request.args.get('n') or msk_now().month)
    except (TypeError, ValueError):
        period_n = msk_now().month
    try:
        wb, period_label = export_dieback_summary_workbook(project, year, period_type, period_n)
    except ValueError as exc:
        flash(str(exc))
        return redirect(url_for('production.project_detail', project_id=project.id, seed_tab='seedTabDieback'))
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    safe = period_label.replace(' ', '_')
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"vypad_svod_{project.id}_{safe}.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


@bp.route('/project/<int:project_id>/yard_stock.xlsx')
@login_required
def seedling_yard_stock_export(project_id):
    denied = _check_production_access()
    if denied:
        return denied
    project = Project.query.get_or_404(project_id)
    from app.seedlings import export_yard_stock_workbook
    try:
        wb = export_yard_stock_workbook(project)
    except ValueError as exc:
        flash(str(exc))
        return redirect(url_for('production.project_detail', project_id=project.id))
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    # Только ASCII в Content-Disposition — иначе YaBrowser/Windows дают «кракозябры» в имени файла.
    filename = f'yard_stock_p{project.id}.xlsx'
    return send_file(
        buf,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
