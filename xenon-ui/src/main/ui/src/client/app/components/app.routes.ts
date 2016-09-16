// angular
import { Routes } from '@angular/router';

// app
import { AboutRoutes } from './about/about.routes';
import { LoginRoutes } from './login/login.routes';
import { MainRoutes } from './main/main.routes';

export const routes: Routes = [
    ...MainRoutes,
    ...LoginRoutes,
    ...AboutRoutes,
    { path: '', redirectTo: '/main/dashboard', pathMatch: 'full' }
];
