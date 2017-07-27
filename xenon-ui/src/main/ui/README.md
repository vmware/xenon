# XENON UI

A developer-centric application that makes it easier to navigate around the xenon nodes and clusters, and monitor the documents.

# Table of Contents

- [Introduction](#introduction)
    - [Availability](#availability)
    - [Available Features](#available-features)
    - [Planned Improvements](#planned-improvements)
- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Usage](#usage)
    - [NativeScript App](#nativescript-app)
    - [Electron App](#electron-app)
    - [Testing](#testing)
    - [Web Configuration Options](#web-configuration-options)
    - [Directory Structure](#directory-structure)
    - [Dependencies & Integrations](#dependencies-integrations)
- [Cut a Release](#cut-a-release)
    - [Release as a web application](#release-as-a-web-application)
    - [Release as a desktop application](#release-as-a-desktop-application)
- [Contributing](#contributing)


# Introduction

Xenon UI is built for one purpose: make your Xenon development experience MUCH better. It provides a rich set of features that can help you:
- Debug your Xenon-based system by organizing various documents and visualizations them in a meaningful way.
- Navigate to any node within the node group and browse node-specific document contents.
- Make REST calls with test payloads.
- Generate query tasks interactively.
- Trace operations.
- Extract logs that are specific to each node.

The application is still in its early stage of development and is expected to iterate/change rapidly.

#### Availability

Xenon UI comes as a web application with every Xenon jar after 0.9.6 and is available in /core/ui/default. In future we will also consider distributing it as a standalone desktop application.

#### Available Features

- Node Selector: displays all the peer nodes and their system info in the current group, and you can select a specific node so the UI shows all the information relevant to this node.
- Dashboard: shows stats for CPU, Memory and Disk usage, and logs.
- Lists all the available factory services and their status and options, as well as detailed information of child services under the factory services.
- Create, Edit(PATCH/PUT) and Delete child services.
- Logs for each node.
- Query UI: allows building queries using either the interactive "Query Builder" or JSON editor, and displays the results right below.
- Operation Tracing: provides a visual way for you to see what operation hits which service at what time. This is very helpful when debugging Xenon’s asynchronous operations.
- Login/logout mechanism.
- (PARTIAL) i18n support

#### Planned Improvements

- Reactive: right now data all come from one off http calls, no change will be reflected on the UI without page refresh. Need some work here.
- Node Selector: Need to cover more complex topology scenarios.
- Dashboard improvements: Aggregated child service stats and index usage stats needs to be added to the dashboard.
- Form improvements: UX revisit and validation on Create, Edit and Delete child service model forms.
- System configuration: should allow edits of some system properties.
- Performance: definitely need some tuning.
- Improvements/fixes based on your feedback.
- Browser compatibility: as of now the UI has only been developed and tested against Chrome, need to make it work cross browser.

# Getting Started

## Prerequisites

* node v5.x.x or higher and npm 3 or higher.

* To run the NativeScript app:

```bash
npm install -g nativescript
npm install -g typescript
```
## Usage

```bash
# install the project's dependencies
$ npm install
# fast install (via Yarn, https://yarnpkg.com)
$ yarn install  # or yarn

# watches your files and uses livereload by default
$ npm start
# api document for the app
# npm run build.docs

# generate api documentation
$ npm run compodoc
$ npm run serve.compodoc

# to start deving with livereload site and coverage as well as continuous testing
$ npm run start.deving

# dev build
$ npm run build.dev
# prod build
$ npm run build.prod
```

## Special Note About AoT

**Note** that AoT compilation requires **node v6.5.0 or higher** and **npm 3.10.3 or higher**.

In order to start the seed with AoT use:

```bash
# prod build with AoT compilation, will output the production application in `dist/prod`
# the produced code can be deployed (rsynced) to a remote server
$ npm run build.prod.aot
```

## NativeScript/Mobile App

The mobile app is provided via [NativeScript](https://www.nativescript.org/), an open source framework for building truly native mobile apps.

#### Setup

```
npm install -g nativescript
```

#### Dev Workflow

You can make changes to files in `src/client/app` or `nativescript/src/app` folders. A symbolic link exists between the web `src/client/app` and the `nativescript/src/app` folder so changes in either location are mirrored because they are the same directory inside.

Create `.tns.html` and `.tns.scss` NativeScript view files for every web component view file you have. You will see an example of the `app.component.html` as a [NativeScript view file here](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/app/components/app.component.tns.html).

The root module for the mobile app is `nativescript/src/native.module.ts`: `NativeModule`.

#### Run

```
iOS:                      npm run start.ios   
iOS (device):             npm run start.ios.device

// or...

Android:                      npm run start.android
Android (device):             npm run start.android.device
```

* Requires an image setup via AVD Manager. [Learn more here](http://developer.android.com/intl/zh-tw/tools/devices/managing-avds.html) and [here](https://github.com/NativeScript/nativescript-cli#the-commands).

OR...

* [GenyMotion Android Emulator](https://www.genymotion.com/)

##### Building with Webpack for release builds

Create AoT builds for deployment to App Store and Google Play.

```
Android:   npm run build.android
iOS:       npm run build.ios
```

## Electron App

The desktop app is provided via [Electron](http://electron.atom.io/), cross platform desktop apps
with JavaScript, HTML, and CSS.

#### Develop

```
Mac:      npm run start.desktop
Windows:  npm run start.desktop.windows
```

#### Develop with livesync
```
Mac:      npm run start.livesync.desktop
Windows:  npm run start.livesync.desktop.windows
```

#### Release: Package Electron App for Mac, Windows or Linux

```
Mac:      npm run build.desktop.mac
Windows:  npm run build.desktop.windows
Linux:    npm run build.desktop.linux
```

## Running tests

```bash
$ npm test

# Development. Your app will be watched by karma
# on each change all your specs will be executed.
$ npm run test.watch
# NB: The command above might fail with a "EMFILE: too many open files" error.
# Some OS have a small limit of opened file descriptors (256) by default
# and will result in the EMFILE error.
# You can raise the maximum of file descriptors by running the command below:
$ ulimit -n 10480


# code coverage (istanbul)
# auto-generated at the end of `npm test`
# view coverage report:
$ npm run serve.coverage

# e2e (aka. end-to-end, integration) - In three different shell windows
# Make sure you don't have a global instance of Protractor

# npm install webdriver-manager <- Install this first for e2e testing
# npm run webdriver-update <- You will need to run this the first time
$ npm run webdriver-start
$ npm run serve.e2e
$ npm run e2e

# e2e live mode - Protractor interactive mode
# Instead of last command above, you can use:
$ npm run e2e.live
```
You can learn more about [Protractor Interactive Mode here](https://github.com/angular/protractor/blob/master/docs/debugging.md#testing-out-protractor-interactively)

## Web Configuration Options

Default application server configuration

```javascript
var PORT             = 5000;
var LIVE_RELOAD_PORT = 4002;
var DOCS_PORT        = 4003;
var APP_BASE         = '/';
```

Configure at runtime

```bash
npm start -- --port 8080 --reload-port 4000 --base /my-app/
```

## Directory Structure

- `nativescript`: Root of this directory is reserved for mobile app.
  - `src`: mobile app src.
    - `app`: Symbolic link of shared code from web app.
    - `App_Resources`: iOS and Android platform specific config files and images.
    - `mobile`: Mobile specific services, etc. Build out mobile specific services here as well as overrides for web services that need to be provided for in the mobile app. **Safe to import {N} modules here.**
    - [native.module.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/nativescript/src/native.module.ts): Root module for the mobile app provided by NativeScript. Override/provide native mobile implementations of services here.
- `src/client`: Root of this directory is reserved for web and desktop.
  - `app`: All the code in this directory is shared with the mobile app via a symbolic link.
    - `components`: Reserved for primary routing components. Since each app usually has it's own set of unique routes, you can provide the app's primary routing components here.
    - `shared`: Shared code across all platforms. Reusable sub components, services, utilities, etc.
      - `analytics`: Provides services for analytics. Out of the box, [Segment](https://segment.com/) is configured.
      - `core`: Low level services. Foundational layer.
      - `electron`: Services specific to electron integration. Could be refactored out in the future since this is not needed to be shared with the mobile app.
      - `i18n`: Various localization features.
      - `ngrx`: Central ngrx coordination. Brings together state from any other feature modules etc. to setup the initial app state.
      - `sample`: Just a sample module pre-configured with a scalable ngrx setup.
      - `test`: Testing utilities. This could be refactored into a different location in the future.
  - `assets`: Various locale files, images and other assets the app needs to load.
  - `css`: List of the main style files to be created via the SASS compilation (enabled by default).
  - `scss`: Partial SASS files - reserved for things like `_variables.scss` and other imported partials for styling.
  - [index.html](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/index.html): The index file for the web and desktop app (which share the same setup).
  - [main.desktop.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/main.desktop.ts): The  file used by Electron to start the desktop app.
  - [main.web.prod.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/main.web.prod.ts): Bootstraps the AoT web build. *Generally won't modify anything here*
  - [main.web.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/main.web.ts): Bootstraps the development web build. *Generally won't modify anything here*
  - [package.json](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/package.json): Used by Electron to start the desktop app.
  - [system-config.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/system-config.ts): This loads the SystemJS configuration defined [here](https://github.com/NathanWalker/angular-seed-advanced/blob/master/tools/config/seed.config.ts#L397) and extended in your own app's customized [project.config.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/tools/config/project.config.ts).
  - [tsconfig.json](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/tsconfig.json): Used by [compodoc](https://compodoc.github.io/compodoc/) - The missing documentation tool for your Angular application - to generate api docs for your code.
  - [web.module.ts](https://github.com/NathanWalker/angular-seed-advanced/blob/master/src/client/web.module.ts): The root module for the web and desktop app.
- `src/e2e`: Integration/end-to-end tests for the web app.

## Dependencies & Integrations

This is an [Angular 2](https://angular.io/) application built on top of [Nathan Walker's](https://github.com/NathanWalker) [angular2-seed-advanced](https://github.com/NathanWalker/angular2-seed-advanced).

#### Core Tech Stacks
- [Angular 2](https://angular.io/) 2.2
- [Bootstrap 4](http://v4-alpha.getbootstrap.com/) Alpha 4
- [jQuery](http://jquery.com/) 3
- [Chart.js](http://www.chartjs.org/)
- [D3](https://d3js.org/) 3
- [CodeMirror](http://codemirror.net/) 5
- [moment](http://momentjs.com/) 2.14
- [Font Awesome](http://fontawesome.io/) 4.7

#### Integrations
- [ngrx/store](https://github.com/ngrx/store) RxJS powered state management, inspired by **Redux**
- [ngrx/effects](https://github.com/ngrx/effects) Side effect model for @ngrx/store
- [ng2-translate](https://github.com/ocombe/ng2-translate) for i18n
  - Usage is optional but on by default
  - Up to you and your team how you want to utilize it. It can be easily removed if not needed.
- [angulartics2](https://github.com/angulartics/angulartics2) Vendor-agnostic analytics for Angular2 applications.
  - Out of box support for [Segment](https://segment.com/)
    - When using the seed, be sure to change your `write_key` [here](https://github.com/NathanWalker/angular2-seed-advanced/blob/master/src/client/index.html#L24)
  - Can be changed to any vendor, [learn more here](https://github.com/angulartics/angulartics2#supported-providers)
- [lodash](https://lodash.com/) Helps reduce blocks of code down to single lines and enhances readability
- [NativeScript](https://www.nativescript.org/) cross platform mobile (w/ native UI) apps. [Setup instructions here](#nativescript-app).
- [Electron](http://electron.atom.io/) cross platform desktop apps (Mac, Windows and Linux). [Setup instructions here](#electron-app).

#### Sync to the latest seed project changes

Due to the amount of changes introduced in each Angular 2/4 release, please be super careful when you decide to sync to the latest changes from the [seed project](https://github.com/NathanWalker/angular2-seed-advanced), which will most likely bump up the Angular version and the underlying dependencies.

Last Change Sync'd: 6e7a949e60194abfe3e8fe7c059e97774338bf04 on 07/26/2017

# Cut a Release

Since Xenon UI is independent from the rest of the Xenon, it has its own version and release cycle.

## Release as a web application

This is the default option which let Xenon to host the UI in /core/ui/default.

#### Produce a production build

In the current `ui` directory:

```bash
npm run build.prod
```

It should generate a `prod` folder under `dist`.

#### Clean up the old files

Go to `xenon/xenon-ui/src/main/resources/ui/com/vmware/xenon/ui/UiService`, run

```bash
rm -r *
rm -r .*
```

#### Deploy the new build

In the current `ui` directory:

```bash
cp -a dist/prod/ ../resources/ui/com/vmware/xenon/ui/UiService
```

#### Rebuild Xenon

Run `mvn clean install` under `xenon` directory

## Release as a desktop application

TBD

# Contributing

Please see the [CONTRIBUTING](https://github.com/vmware/xenon/blob/master/CONTRIBUTING.md) file for guidelines.
