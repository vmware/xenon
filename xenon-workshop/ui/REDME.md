# Getting Started with Xenon UI and Clarity

In this lab, we will show you how to build a [Clarity]((https://vmware.github.io/clarity/)) and [Angular](https://angular.io) based UI and hosted from Xenon.

In development time, we will leverage [Angular CLI](https://github.com/angular/angular-cli) (included in Clarity Seed, but you still need to have it in your system in order to build) to run and debug the application, and use a proxy to communicate with the Xenon host.
Then we will create a production build and deploy the `dist` folder to Xenon so it can be hosted from there.

## Prerequisite

- [Node.js](https://nodejs.org)
- [Angular CLI](https://github.com/angular/angular-cli)

## Step 1. Create UI Service

In the src folder, we have already provided a skeleton Xenon host with RootNamespaceService and Example service started,
all you need to do is to create a UI service and add start it when the host starts.

1. Create UI service class

Under `src/main/java/com/vmware/xenon/workshop`, create a new Java class called `ClarityUiService.java`,
and add the following snippet into it:

```java
package com.vmware.xenon.workshop;

import com.vmware.xenon.services.common.UiContentService;

/**
 * Simple UiContentService
 */
public class ClarityUiService extends UiContentService {
    public static final String SELF_LINK = "/clarity";
}
```

2. Start UI service

In `UiDemo.java`, add the following lines to the `main`:

```java
// start the UiService
host.startService(new ClarityUiService());
```

At this point, when the host starts, you will have a UI service running under [http://localhost:8000/clarity](http://localhost:8000/clarity).
But there won't be any content yet. We will add them in the following steps.

3. Build and run Xenon host

From command line, `cd` to the UI lab folder `/xenon-workshop/ui/`, run:

```bash
mvn clean install
java -jar target/workshop-ui-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

4. Create child services

Now let's create a few child services for ExampleService, for testing purposes. In your command line, run:

```bash
for i in {1..10}; do curl -s -H "Content-Type: application/json" -X POST http://localhost:8000/core/examples -d "{'name':'foo-${i}','documentSelfLink':'/foo-${i}'}" > /dev/null; done
```

At this point, if you go to [http://localhost:8000/core/examples](http://localhost:8000/core/examples) from your browser, you will see 10 child services listed under `documentLinks`.

## Step 2. Clone Clarity Seed

1. Clone the seed app to somewhere OUTSIDE the current Xenon project:

```bash
git clone https://github.com/vmware/clarity-seed.git
```

2. Install the dependencies:

```bash
npm install
```

NOTE: For more info about Clarity, check out their website at [https://vmware.github.io/clarity/](https://vmware.github.io/clarity/)

## Step 3. Setup proxy for development

1. Under the `clarity-seed` folder, create a new file called `proxy.conf.json` with content:

```javascript
{
    "/api": {
        "target": "http://localhost:8000",
        "secure": false,
        "pathRewrite": {"/api" : ""}
    }
}
```

What this file does is mapping any request to `http://localhost:4200/api/**`(where the UI is hosted during development time)
to `http://localhost:8000/**`, in order to retrieve data from Xenon.

2. Edit `package.json` file's start script to be:

```bash
"start": "ng serve --proxy-config proxy.conf.json",
```

3. Start node server in your command line:

```bash
npm start
```

At this point the UI is served at [http://localhost:4200/](http://localhost:4200/), with some default content coming from Clarity.

## Step 4. Modify the Clarity Seed to get data from Xenon

In this lab, we will retrieve ExampleService's child service document links, and display them as cards on the home page.

1. Use different base URL for dev and prod

Under `src/app` folder, open `app.module.ts`, add the following imports on the top of the file (order doesn't matter):

```javascript
import { APP_BASE_HREF } from '@angular/common';
import { environment } from '../environments/environment';
```

Scroll down to the `@NgModule` section, modify `providers` to be:
 
```javascript
providers: [{
    provide: APP_BASE_HREF,
    useValue: environment.production ? '/clarity' : ''
}],
```

This configuration tells the app to use different base url for development and production environment.
(Recall that during development the app is served by Angular CLI on [http://localhost:4200/](http://localhost:4200/), and in production
the app is served by Xenon host under [http://localhost:8000/clarity](http://localhost:8000/clarity)).

2. Use hash location strategy for routing

Open `app.routing.ts`, add `{ useHash: true }` as the second parameter to `RouterModule.forRoot`, like this:

```javascript
export const ROUTING: ModuleWithProviders = RouterModule.forRoot(ROUTES, { useHash: true });
```

Now Xenon host knows how to load UI resources properly.

3. Get ExampleService document links and display them on the UI

Open `src/app/home`, replace the code in `home.component.ts` with this:
 
```javascript
import { Component } from "@angular/core";
import { Http, Headers, RequestOptions, Response } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';

import { environment } from '../../environments/environment';

@Component({
    styleUrls: ['./home.component.scss'],
    templateUrl: './home.component.html',
})
export class HomeComponent {
    exampleServices: any[];

    constructor(private http: Http) {
        this.getExmapleServices();
    }

    getExmapleServices(): void {
        // Switch between /api/core/example and /core/example depends on
        // whether the environment is development or production
        let prefix: string = environment.production ? '' : '/api';
        let url: string = prefix + '/core/examples';

        this.http.get(url)
            .map((res: Response) => {
                return res.json();
            })
            .subscribe((document: any) => {
                this.exampleServices = document.documentLinks;
            });
  }
}
```

In `home.component.html`, replace the entire `clr-dropdown` element with cards:

```html
<div class="row">
    <div class="col-lg-3 col-md-4 col-sm-6 col-xs-12"
        *ngFor="let service of exampleServices">
        <a class="card">
            <div class="card-block">
                <p class="card-text">
                    {{ service }}
                </p>
            </div>
        </a>
    </div>
</div>
```

By now in your [http://localhost:4200/](http://localhost:4200/) you should see the a bunch of cards show up with the child service URLs.

## Step 5. Deploy the UI to Xenon Host

It's time to deploy our new shiny UI to the Xenon host and have it serve the UI for us, instead of relying on the Angular CLI server.

1. Create a production build for the UI

`cd` into your `clarity-seed` folder in command line, run:

```bash
ng build --prod --base-href /clarity/ --deploy-url /clarity/
```

By specifying both `--base-href` and `--deploy-url` to `/clarity/`, we make sure all the resources on the UI side have proper paths when deployed to Xenon.

2. Copy the production files to Xenon host

In the UI lab folder, you should see a `src/main/resources` folder,  keep in mind the folder structure under here should contain a `ui` folder which in turn includes
a path that matches the `ClarityUiService.java`'s package structure. In this case we have `com/vmware/xenon/workshop/ClarityUiService`, and all the UI files should
be placed here, and an `index.html` is required.

Assuming your command line is still under `clarity-seed`, run:

```bash
cp -a dist/ {{PATH_TO_YOUR_XENON_PROJECT}}/xenon-workshop/ui/src/main/resources/ui/com/vmware/xenon/workshop/ClarityUiService/
```

3. Rebuild Xenon host and restart

Now `cd` to the UI lab folder `/xenon-workshop/ui/`, run:

```bash
mvn clean install
java -jar target/workshop-ui-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

Finally, open [http://localhost:8000/clarity](http://localhost:8000/clarity) in your browser, and enjoy!