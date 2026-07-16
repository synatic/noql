# NoQL SQL to Mongo

This doc project is built on [mkdocs-material](https://squidfunk.github.io/mkdocs-material/)

Project board at [https://github.com/orgs/synatic/projects/11](https://github.com/orgs/synatic/projects/11)

## Setup

Run all commands from this directory (the docs directory)

Download the mkdocs-material docker image:

For Intel Macs / Windows / Linux:

```bash
docker pull squidfunk/mkdocs-material
```

For Apple Silicon Macintoshes:

```bash
docker pull ghcr.io/afritzler/mkdocs-material
```

## Developing

To create a live preview while writing, run the following in the same directory as the `mkdocs.yml` file:

Intel Macs / Windows / Linux:

```bash
docker run --rm -it -p 8000:8000 -v ${PWD}:/docs squidfunk/mkdocs-material
```

Apple Silicon Macintoshes:

```bash
docker run --rm -it -p 8000:8000 -v ${PWD}:/docs ghcr.io/afritzler/mkdocs-material
```

The docs will be available at [http://localhost:8000](http://localhost:8000)

### Developing without Docker

Alternatively, if you have Python installed locally:

```bash
pip install mkdocs-material
```

Then, from the repository root, run:

```bash
npm run docs:serve
```

This builds the JS bundle (`npm run build`), copies it into `docs/docs/javascripts/noql/index.js` (mirroring what CI does on release), and starts `mkdocs serve` from the `docs` directory. The docs will be available at [http://localhost:8000](http://localhost:8000).

Note: if you change files under `lib/`, re-run `npm run docs:serve` (or `npm run build:docs` in another terminal) to rebuild the bundle — `mkdocs serve`'s live reload only watches the `docs` directory, but it will pick up the refreshed bundle file automatically once copied.

### Publishing

Pushing to the `main` branch will automatically publish the docs to [https://noql.synatic.dev](https://noql.synatic.dev)

This is a github pages site, hosted on the `gh-pages` branch of this repo.

### Quirks and known issues

-   For some reason, the `{% block footer %}{% endblock %}` doesn't render the footer on the overridden home page. I've had to manually copy the footer into the home page for now. Remember to update it for date and social links.
