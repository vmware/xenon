// angular
import { Routes } from '@angular/router';

// app
import { LoginRoutes } from './login/login.routes';
import { MainRoutes } from './main/main.routes';

export const routes: Routes = [
    ...MainRoutes,
    ...LoginRoutes,
    { path: '', redirectTo: '/main/dashboard', pathMatch: 'full' }
];
