import { Route } from '@angular/router';

import { AuthenticationGuard } from '../../frameworks/app/services/index';

import { MainComponent, DashboardGridComponent, ProcessLogComponent,
    QueryComponent, ServiceDetailComponent, ServiceGridComponent } from './index';

export const MainRoutes: Route[] = [
    {
        path: 'main',
        component: MainComponent,
        children: [
            {
                path: '',
                redirectTo: 'dashboard',
                pathMatch: 'full'
            },
            {
                path: 'dashboard',
                component: DashboardGridComponent,
                canActivate: [AuthenticationGuard]
            },
            {
                path: 'service',
                component: ServiceGridComponent,
                canActivate: [AuthenticationGuard]
            },
            {
                path: 'service/:id',
                component: ServiceDetailComponent,
                canActivate: [AuthenticationGuard]
            },
            {
                path: 'service/:id/:instanceId',
                component: ServiceDetailComponent,
                canActivate: [AuthenticationGuard]
            },
            {
                path: 'process-log',
                component: ProcessLogComponent,
                canActivate: [AuthenticationGuard]
            },
            {
                path: 'query',
                component: QueryComponent,
                canActivate: [AuthenticationGuard]
            }
        ]
    }
];
