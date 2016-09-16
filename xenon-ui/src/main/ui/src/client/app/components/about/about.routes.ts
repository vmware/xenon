// angular
import { Route } from '@angular/router';

import { AboutDocumentComponent, AboutIntroComponent, AboutComponent } from './index';

export const AboutRoutes: Route[] = [
    {
        path: 'about',
        component: AboutComponent,
        children: [
            {
                path: '',
                redirectTo: 'intro',
                pathMatch: 'full'
            },
            {
                path: 'intro',
                component: AboutIntroComponent
            },
            {
                path: 'document',
                component: AboutDocumentComponent
            }
        ]
    }
];
