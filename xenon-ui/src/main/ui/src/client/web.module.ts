// angular
import { NgModule } from '@angular/core';
import { APP_BASE_HREF } from '@angular/common';
import { BrowserModule } from '@angular/platform-browser';
import { RouterModule } from '@angular/router';

// app
import { routes } from './app/components/app.routes';
import { AppComponent,
    // login
    StarCanvasComponent, LoginComponent,

    // main
    DashboardCardComponent, DashboardGridComponent, ProcessLogComponent,
    QueryClauseNestedComponent, QueryClauseComponent, QueryNestedComponent,
    QueryResultDetailComponent, QuerySpecReferenceComponent, QueryComponent,
    ServiceCardComponent, ServiceDetailComponent, ServiceGridComponent,
    ServiceInstanceDetailComponent, MainComponent } from './app/components/index/';

// feature modules
import { CoreModule } from './app/frameworks/core/core.module';
import { AnalyticsModule } from './app/frameworks/analytics/analytics.module';
import { MultilingualModule } from './app/frameworks/i18n/multilingual.module';
import { AppModule } from './app/frameworks/app/app.module';

// config
import { Config, WindowService, ConsoleService } from './app/frameworks/core/index';
Config.PLATFORM_TARGET = Config.PLATFORMS.WEB;
if (String('<%= ENV %>') === 'dev') {
  // only output console logging in dev mode
  Config.DEBUG.LEVEL_4 = true;
}

// sample config (extra)
import { AppConfig } from './app/frameworks/app/index';
import { MultilingualService } from './app/frameworks/i18n/index';
// custom i18n language support
MultilingualService.SUPPORTED_LANGUAGES = AppConfig.SUPPORTED_LANGUAGES;

if (String('<%= TARGET_DESKTOP %>') === 'true') {
    Config.PLATFORM_TARGET = Config.PLATFORMS.DESKTOP;
}

@NgModule({
    imports: [
        BrowserModule,
        CoreModule.forRoot([
            { provide: WindowService, useValue: window },
            { provide: ConsoleService, useValue: console }
        ]),
        // Both web and desktop (electron) need to use hash
        RouterModule.forRoot(routes, { useHash: true }),
        AnalyticsModule,
        MultilingualModule,

        AppModule
    ],
    declarations: [ AppComponent,
        // login
        StarCanvasComponent,
        LoginComponent,

        // main
        DashboardCardComponent,
        DashboardGridComponent,
        ProcessLogComponent,
        QueryClauseNestedComponent,
        QueryClauseComponent,
        QueryNestedComponent,
        QueryResultDetailComponent,
        QuerySpecReferenceComponent,
        QueryComponent,
        ServiceCardComponent,
        ServiceDetailComponent,
        ServiceGridComponent,
        ServiceInstanceDetailComponent,
        MainComponent ],
    providers: [
        {
            provide: APP_BASE_HREF,
            useValue: '<%= APP_BASE %>'
        }
    ],
    bootstrap: [ AppComponent ]
})

export class WebModule { }
