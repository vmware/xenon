import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, Router,
    ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';

import { AuthenticationService } from './authentication.service';

@Injectable()
export class AuthenticationGuard implements CanActivate, CanActivateChild {
    constructor(
        private authenticationService: AuthenticationService,
        private router: Router) {}

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        return this.authenticationService.isLoggedIn().map((isLoggedIn: boolean) => {
                if (isLoggedIn) {
                    return true;
                }

                // Store the attempted URL for redirecting
                this.authenticationService.redirectUrl = state.url;

                // Navigate to the login page
                this.router.navigate(['/login']);
                return false;
            }).catch(() => {
                // Store the attempted URL for redirecting
                this.authenticationService.redirectUrl = state.url;

                // Navigate to the login page
                this.router.navigate(['/login']);
                return Observable.of(false);
            });
    }

    canActivateChild(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        return this.canActivate(route, state);
    }
}
