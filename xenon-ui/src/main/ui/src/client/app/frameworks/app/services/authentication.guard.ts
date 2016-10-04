import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, Router,
    ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { Observable } from 'rxjs/Observable';

import { AuthenticationService } from './authentication.service';

@Injectable()
export class AuthenticationGuard implements CanActivate, CanActivateChild {
    constructor(
        private _authenticationService: AuthenticationService,
        private _router: Router) {}

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        return this._authenticationService.isLoggedIn().map((isLoggedIn: boolean) => {
                if (isLoggedIn) {
                    return true;
                }

                // Store the attempted URL for redirecting
                this._authenticationService.redirectUrl = state.url;

                // Navigate to the login page
                this._router.navigate(['/login']);
                return false;
            }).catch(() => {
                // Store the attempted URL for redirecting
                this._authenticationService.redirectUrl = state.url;

                // Navigate to the login page
                this._router.navigate(['/login']);
                return Observable.of(false);
            });
    }

    canActivateChild(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        return this.canActivate(route, state);
    }
}
