// angular
import { NgModule } from '@angular/core';
import { APP_BASE_HREF } from '@angular/common';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { Http } from '@angular/http';

// libs
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { TranslateLoader } from '@ngx-translate/core';
import { InfiniteScrollModule } from 'ngx-infinite-scroll';

// app
import { routes } from './app/components/app.routes';
import { AppComponent,
    // login
    StarCanvasComponent, LoginComponent,

    // main
    DashboardCardComponent, DashboardGridComponent,
    OperationTracingChartDetailComponent, OperationTracingChartComponent,
    OperationTracingClauseComponent, OperationTracingComponent,
    ProcessLogComponent,
    QueryClauseNestedComponent, QueryClauseComponent, QueryNestedComponent,
    QueryResultDetailComponent, QuerySpecReferenceComponent, QueryComponent,
    ServiceCardComponent, ServiceDetailComponent, ServiceGridComponent,
    ChildServiceDetailComponent, MainComponent } from './app/components/index';

// feature modules
import { WindowService, StorageService, ConsoleService, createConsoleTarget, provideConsoleTarget,
    LogTarget, LogLevel, ConsoleTarget } from './app/modules/core/services/index';
import { CoreModule, Config } from './app/modules/core/index';
import { AnalyticsModule } from './app/modules/analytics/index';
import { MultilingualModule, Languages, translateLoaderFactory, MultilingualEffects } from './app/modules/i18n/index';
import { AppReducer } from './app/modules/ngrx/index';
import { AppModule } from './app/modules/app/app.module';

// config
Config.PLATFORM_TARGET = Config.PLATFORMS.WEB;
if (String('<%= BUILD_TYPE %>') === 'dev') {
    // only output console logging in dev mode
    Config.DEBUG.LEVEL_4 = true;
}

if (String('<%= TARGET_DESKTOP %>') === 'true') {
    Config.PLATFORM_TARGET = Config.PLATFORMS.DESKTOP;
}

declare var window, console, localStorage;

// For AoT compilation to work:
export function win() {
    return window;
}
export function storage() {
    return localStorage;
}
export function cons() {
    return console;
}
export function consoleLogTarget(consoleService: ConsoleService) {
    return new ConsoleTarget(consoleService, { minLogLevel: LogLevel.Debug });
}

let DEV_IMPORTS: any[] = [];

if (String('<%= BUILD_TYPE %>') === 'dev') {
    DEV_IMPORTS = [
        ...DEV_IMPORTS,
        StoreDevtoolsModule.instrumentOnlyWithExtension()
    ];
}

@NgModule({
    imports: [
        BrowserModule,
        BrowserAnimationsModule,
        FormsModule,
        CoreModule.forRoot([
            { provide: WindowService, useFactory: (win) },
            { provide: StorageService, useFactory: (storage) },
            { provide: ConsoleService, useFactory: (cons) },
            { provide: LogTarget, useFactory: (consoleLogTarget), deps: [ConsoleService], multi: true }
        ]),
        // Both web and desktop (electron) need to use hash
        RouterModule.forRoot(routes, { useHash: true }),
        AnalyticsModule,
        MultilingualModule.forRoot([{
            provide: TranslateLoader,
            deps: [Http],
            useFactory: (translateLoaderFactory)
        }]),
        StoreModule.provideStore(AppReducer),
        StoreDevtoolsModule.instrumentOnlyWithExtension(),
        InfiniteScrollModule,

        AppModule,

        EffectsModule.run(MultilingualEffects)
    ],
    declarations: [AppComponent,
        // login
        StarCanvasComponent,
        LoginComponent,

        // main
        DashboardCardComponent,
        DashboardGridComponent,
        OperationTracingChartDetailComponent,
        OperationTracingChartComponent,
        OperationTracingClauseComponent,
        OperationTracingComponent,
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
        ChildServiceDetailComponent,
        MainComponent],
    providers: [
        {
            provide: APP_BASE_HREF,
            useValue: '<%= APP_BASE %>'
        },
        // override with supported languages
        {
            provide: Languages,
            useValue: Config.GET_SUPPORTED_LANGUAGES()
        }
    ],
    bootstrap: [AppComponent]
})

export class WebModule { }
