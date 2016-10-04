// angular
import { Injectable }  from '@angular/core';
import { Http, Headers, RequestOptions, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import { CookieService } from 'angular2-cookie/core';

import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import 'rxjs/add/observable/throw';

import { URL } from '../enums/index';
import { ServiceHostState } from '../interfaces/index';

const COOKIE_KEY: string = 'xenon-auth-cookie';

@Injectable()
export class AuthenticationService {

    redirectUrl: string;

    private _isAuthorizationEnabled: boolean;

    constructor (
        private _http: Http,
        private _cookieService: CookieService) {}

    isLoggedIn(): Observable<boolean> {
        // Only check about this once
        if (_.isUndefined(this._isAuthorizationEnabled)) {
            // Make a test call to nodeGroupService and see if the instance has
            // authorization enabled.
            return this._http.get(URL.API_PREFIX + URL.CoreManagement)
                .map((res: Response) => {
                    var response: ServiceHostState = res.json();
                    this._isAuthorizationEnabled = response.isAuthorizationEnabled;
                    return true;
                })
                .catch((err: Response) => {
                    var error: any = err.json();
                    if (error.statusCode === 401 || error.statusCode === 403) {
                        this._isAuthorizationEnabled = true;

                        return this._isCookieValid();
                    }
                    return Observable.of(false);
                });
        }

        // If ruthorization is disabled, always logged in.
        if (!this._isAuthorizationEnabled) {
            return Observable.of(true);
        }

        return this._isCookieValid();
    }

    login(username: string, password: string): Observable<any> {
        var encryptedCredentials = btoa(`${username}:${password}`);

        var headers = new Headers({
            'Content-Type': 'application/json',
            'Authorization': `Basic ${encryptedCredentials}`
        });
        var options = new RequestOptions({ headers: headers });

        return this._http.post(
                URL.API_PREFIX + URL.Authentication,
                JSON.stringify({
                    requestType: 'LOGIN'
                }),
                options)
            .map((res: Response) => {
                return res.json();
            })
            .catch(this._onError);
    }

    logout(): void {
        this._cookieService.remove(COOKIE_KEY);
    }

    private _isCookieValid(): Observable<boolean> {
        // Requires authentication
        var cookie: string = this._cookieService.get(COOKIE_KEY);

        // If either of the cookie is not available or the cookie expired, return false.
        if (!cookie) {
            return Observable.of(false);
        }

        return Observable.of(true);
    }

    private _onError(error: Response) {
        return Observable.throw(error.json() || 'Server error');
    }
}
