// angular
import { NgModule, Optional, SkipSelf } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';

// libs
import { StoreModule } from '@ngrx/store';
import { CookieService } from 'angular2-cookie/core';

// app
import { AlertComponent, CodeEditorComponent, NavbarComponent, NodeCanvasComponent,
    NodeInfoPanelComponent, NodeSelectorComponent, NotificationComponent,
    PropertyTableComponent } from './components/index';
import { FilterByNamePipe } from './pipes/index';
import { AuthenticationGuard, AuthenticationService, BASE_SERVICE_PROVIDERS,
    NodeSelectorService, NotificationService } from './services/index';

import { MultilingualModule } from '../i18n/multilingual.module';
import { multilingualReducer, MultilingualStateI } from '../i18n/index';

// state
export interface AppStoreI {
    i18n: MultilingualStateI;
    names: Array<string>;
};

/**
 * Do not specify providers for modules that might be imported by a lazy loaded module.
 */

@NgModule({
    imports: [
        CommonModule,
        FormsModule,
        RouterModule,
        MultilingualModule,
        StoreModule.provideStore({
            i18n: multilingualReducer
        })
    ],
    declarations: [
        AlertComponent,
        CodeEditorComponent,
        NavbarComponent,
        NodeCanvasComponent,
        NodeInfoPanelComponent,
        NodeSelectorComponent,
        NotificationComponent,
        PropertyTableComponent,
        FilterByNamePipe
    ],
    exports: [
        AlertComponent,
        CodeEditorComponent,
        NavbarComponent,
        NodeCanvasComponent,
        NodeInfoPanelComponent,
        NodeSelectorComponent,
        NotificationComponent,
        PropertyTableComponent,
        FilterByNamePipe
    ],
    providers: [
        AuthenticationGuard,
        AuthenticationService,
        BASE_SERVICE_PROVIDERS,
        CookieService,
        NodeSelectorService,
        NotificationService
    ]
})
export class AppModule {
    constructor( @Optional() @SkipSelf() parentModule: AppModule) {
        if (parentModule) {
            throw new Error('Xenon AppModule already loaded; Import in root module only.');
        }
    }
}
