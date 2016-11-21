// angular
import { NgModule } from '@angular/core';
import { APP_BASE_HREF } from '@angular/common';
import { BrowserModule } from '@angular/platform-browser';
import { RouterModule } from '@angular/router';
import { Http } from '@angular/http';

// libs
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { ConfigLoader, ConfigService } from 'ng2-config';
import { TranslateLoader } from 'ng2-translate';

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
    ServiceInstanceDetailComponent, MainComponent } from './app/components/index/';

// feature modules
import { CoreModule, configFactory } from './app/frameworks/core/core.module';
import { AppReducer } from './app/frameworks/ngrx/index';
import { AnalyticsModule } from './app/frameworks/analytics/analytics.module';
import { MultilingualEffects } from './app/frameworks/i18n/index';
import { MultilingualModule, translateFactory } from './app/frameworks/i18n/multilingual.module';
import { AppModule } from './app/frameworks/app/app.module';

// config
import { Config, WindowService, ConsoleService } from './app/frameworks/core/index';
Config.PLATFORM_TARGET = Config.PLATFORMS.WEB;

if (String('<%= TARGET_DESKTOP %>') === 'true') {
    Config.PLATFORM_TARGET = Config.PLATFORMS.DESKTOP;
}

declare var window, console;

// For AoT compilation to work:
export function win() {
    return window;
}
export function cons() {
    return console;
}

@NgModule({
    imports: [
        BrowserModule,
        CoreModule.forRoot([
            { provide: WindowService, useFactory: (win) },
            { provide: ConsoleService, useFactory: (cons) },
            { provide: ConfigLoader, useFactory: (configFactory) }
        ]),
        // Both web and desktop (electron) need to use hash
        RouterModule.forRoot(routes, { useHash: true }),
        AnalyticsModule,
        MultilingualModule.forRoot([{
            provide: TranslateLoader,
            deps: [Http],
            useFactory: (translateFactory)
        }]),
        StoreModule.provideStore(AppReducer),
        StoreDevtoolsModule.instrumentOnlyWithExtension(),

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
        ServiceInstanceDetailComponent,
        MainComponent],
    providers: [
        {
            provide: APP_BASE_HREF,
            useValue: '<%= APP_BASE %>'
        }
    ],
    bootstrap: [AppComponent]
})

export class WebModule { }
