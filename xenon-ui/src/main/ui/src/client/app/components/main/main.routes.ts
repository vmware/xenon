import { Route } from '@angular/router';

import { AuthenticationGuard } from '../../frameworks/app/services/index';

import { MainComponent, DashboardGridComponent, ProcessLogComponent,
    QueryComponent, ServiceDetailComponent, ServiceGridComponent } from './index';

export const MainRoutes: Route[] = [
    {
        path: 'main',
        component: MainComponent,
        canActivate: [AuthenticationGuard],
        children: [
            {
                path: '',
                canActivateChild: [AuthenticationGuard],
                children: [
                    {
                        path: 'dashboard',
                        component: DashboardGridComponent
                    },
                    {
                        path: 'service',
                        component: ServiceGridComponent
                    },
                    {
                        path: 'service/:id',
                        component: ServiceDetailComponent
                    },
                    {
                        path: 'service/:id/:instanceId',
                        component: ServiceDetailComponent
                    },
                    {
                        path: 'process-log',
                        component: ProcessLogComponent
                    },
                    {
                        path: 'query',
                        component: QueryComponent
                    },
                    {
                        path: '',
                        redirectTo: 'dashboard',
                        pathMatch: 'full'
                    }
                ]
            }
        ]
    }
];
