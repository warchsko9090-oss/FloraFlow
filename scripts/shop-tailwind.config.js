/** Сборка витрины: npx tailwindcss@3.4.17 -c scripts/shop-tailwind.config.js -i scripts/shop-tailwind-input.css -o app/static/vendor/shop-tailwind.css --minify */
module.exports = {
  content: [
    './app/templates/shop/**/*.html',
    './app/templates/public_*.html',
    './app/static/js/shop.js',
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          bg: '#111814',
          bgSoft: '#161D19',
          card: '#1C2420',
          cardHover: '#232B27',
          cream: '#F7F5F0',
          pine: '#111814',
          brass: '#B69A6D',
          brassHover: '#C4AB7E',
          brassDim: '#8A7555',
          graphite: '#C8C5BE',
          muted: '#7A7E78',
          border: '#2E3632',
          borderLight: '#3D4741',
          low: '#D47A45',
        },
      },
      fontFamily: {
        sans: ['Golos Text', 'sans-serif'],
        serif: ['Cormorant', 'serif'],
      },
    },
  },
  plugins: [],
};
