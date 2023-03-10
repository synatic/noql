# NoQL SQL to Mongo 

This doc project is built on [mkdocs-material](https://squidfunk.github.io/mkdocs-material/)

Project board at [https://github.com/orgs/synatic/projects/11](https://github.com/orgs/synatic/projects/11)

## Setup

Run all commands from this directory (the docs directory)

Download the mkdocs-material docker image:

```bash
docker pull squidfunk/mkdocs-material
```

## Developing

### Intel Macs / Windows / Linux
To create a live preview while writing, run:

```bash
docker run --rm -it -p 8000:8000 -v ${PWD}:/docs squidfunk/mkdocs-material
```

### Apple Silicon Macintoshes

```bash
docker run --rm -it -p 8000:8000 -v ${PWD}:/docs ghcr.io/afritzler/mkdocs-material
```

The docs will be available at [http://localhost:8000](http://localhost:8000)

### Publishing

Pushing to the `main` branch will automatically publish the docs to [https://noql.synatic.dev](https://noql.synatic.dev) 

This is a github pages site, hosted on the `gh-pages` branch of this repo.

### Quirks and known issues

* For some reason, the `{% block footer %}{% endblock %}` doesn't render the footer on the overridden home page. I've had to manually copy the footer into the home page for now.