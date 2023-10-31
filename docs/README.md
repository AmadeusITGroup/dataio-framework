# Data IO documentation
The Data IO documentation is powered by the [Jekyll](https://jekyllrb.com/) static website generator 
along with the [Just the Docs](https://just-the-docs.com/) theme.

Useful links:
- To know more about Jekyll: https://jekyllrb.com/docs/
- To know more about Just the Docs: https://just-the-docs.com/


## Installing Jekyll and Bundler

Follow the instructions on [Jekyll official website](https://jekyllrb.com/docs/installation/).

## Updating the documentation
Updating it is as simple as editing the Markdown files in `content/`, and the YAML files in `content/_data/`.
Simply put, the Markdown files are pages and templates, and the YAML files are data used to fill these templates.

The `content/_includes/` directory contains templates that can and should be used in the Markdown files. 
**Note that changes to this directory should be avoided, unless you are very familiar with Jekyll and Just the Docs.**

While contributing, please ensure you are following rigourous naming conventions for your pages and navigation items, 
as well as a **clear** and **precise** language in the documentation of new features, for a more user-friendly experience.

### Documenting a new feature

Let's imagine you need to add documentation for a new streaming pipe called MyDB. You need to:
1. Create a directory for your new pipe's description data: `content/_data/config/pipes/mydb`
2. Create a common.yaml file for global information about your pipe. This file contains the following fields: 
  - name: The name of your pipe
  - description: A short description of your pipe
  - links: An array of items with the following fields: 
    - name: The name of the link
    - url: The url
  - top_warning: A string that will appear as warning  at the top
  - fields: An array of items with the following fields:
    - name: The name of the field to use in the configuration files, e.g. `MyField`
    - mandatory: `Yes` or `No` (if omitted, will display `No`)
    - description: Description of what the field does
    - example: An example for the field, such as `MyField = "myvalue"`
    - default: The default value of the field, e.g. `mydefaultvalue`
  - fields_warning: A string that will appear as warning below the common fields table
3. Create a streaming.yaml file for specific information about streaming configuration for your pipe. Here is an example:
```
input:
  type: com.amadeus.dataio.pipes.mydb.streaming.MyDBInput
  fields:
  - (...)
output:
  type: com.amadeus.dataio.pipes.mydb.streaming.MyDBOutput
  fields:
  - (...)
```
4. Then create a new page: `content/configuration/pipes/mydbpipe.md` with the following content:
```
---
title: MyPipe
layout: default
grand_parent: Configuration
parent: Pipes
---
# MyPipe

{% include pipe_description.md pipe=site.data.config.pipes.mydb %}
```

These steps will first create data that can be used in the documentation, and then create a page that will import the 
pipe_description.md template, and fill it with the data you just created.

Note: this also works for batch components. SImply add a `batch.yaml` file with the same structure as `streaming.yaml`.

## Building and previewing the site locally

Assuming [Jekyll] and [Bundler] are installed on your computer:

1.  Change your working directory to the root directory of your site.

2.  Run `bundle install --deployment`.

3.  Run `bundle exec jekyll serve --livereload` to build your site and preview it at `localhost:4000`.

## Deploying the site
The site is automatically built and deployed from the docs/ directory using GitHub Actions.
