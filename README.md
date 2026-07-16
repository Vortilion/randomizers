# Randomizers

`Randomizers` is an Angular 21 application that hosts multiple Great Western Trail setup randomizers behind a shared dashboard.

Current sub-applications:

- `Argentina` at `/randomizers/gwt-argentina`
- `2nd Edition` at `/randomizers/gwt-2nd-edition`

## Stack

- Angular 21
- Angular Material 21
- Transloco for i18n
- Tailwind CSS 4
- Service worker support for production builds

## Development

Install dependencies and start the local dev server:

```bash
npm install
npm start
```

The app runs at `http://localhost:4200/` by default.

## Scripts

- `npm start`: run the Angular dev server
- `npm run build`: create a standard build
- `npm run build:prod`: create the GitHub Pages production build for `https://vortilion.github.io/randomizers/`
- `npm run watch`: rebuild on file changes in development mode
- `npm test`: run the test suite
- `npm run test:ci`: run tests once without watch mode
- `npm run lint`: run linting

## Routing

The application uses a parent route at `/randomizers`:

- `/randomizers`: dashboard
- `/randomizers/gwt-argentina`: Great Western Trail Argentina randomizer
- `/randomizers/gwt-2nd-edition`: Great Western Trail 2nd Edition randomizer

## Deployment

The production build is configured for GitHub Pages under the `randomizers` repository:

```bash
npm run build:prod
```

The build output is written to `dist/browser/`, and the script also copies `index.html` to `404.html` so client-side routing works on GitHub Pages.

## Notes

- The previous `greatwesterntrail-arg` repository can remain unchanged as a backup.
- This repository now represents the combined multi-randomizer application.
