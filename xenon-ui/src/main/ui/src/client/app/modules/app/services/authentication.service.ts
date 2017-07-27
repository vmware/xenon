// angular
import { Injectable }  from '@angular/core';
import { Http, Headers, RequestOptions, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import * as _ from 'lodash';
import * as moment from 'moment';

import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import 'rxjs/add/observable/throw';

import { URL } from '../enums/index';
import { ServiceHostState } from '../interfaces/index';

const LAST_LOGIN_TIME_IN_SECONDS_STORAGE_KEY: string = 'lastLogInTimeInSec';
const MAX_COOKIE_AGE_IN_SECONDS: number = 3600;

@Injectable()
export class AuthenticationService {

    redirectUrl: string;

    private isAuthorizationEnabled: boolean;

    constructor (
        private http: Http) {}

    isLoggedIn(): Observable<boolean> {
        // Only check about this once
        if (_.isUndefined(this.isAuthorizationEnabled)) {
            // Make a test call to nodeGroupService and see if it has
            // authorization enabled.
            return this.http.get(URL.API_PREFIX + URL.CoreManagement)
                .map((res: Response) => {
                    var response: ServiceHostState = res.json();
                    this.isAuthorizationEnabled = response.isAuthorizationEnabled;
                    return true;
                })
                .catch((err: Response) => {
                    var error: any = err.json();
                    if (error.statusCode === 401 || error.statusCode === 403) {
                        this.isAuthorizationEnabled = true;

                        return this.isCookieValid();
                    }
                    return Observable.of(false);
                });
        }

        // If ruthorization is disabled, always logged in.
        if (!this.isAuthorizationEnabled) {
            return Observable.of(true);
        }

        return this.isCookieValid();
    }

    login(username: string, password: string): Observable<any> {
        var encryptedCredentials = btoa(`${username}:${password}`);

        var headers: Headers = new Headers({
            'Content-Type': 'application/json',
            'Authorization': `Basic ${encryptedCredentials}`
        });
        var options: RequestOptions = new RequestOptions({ headers: headers });

        return this.http.post(
                URL.API_PREFIX + URL.Authentication,
                JSON.stringify({
                    requestType: 'LOGIN'
                }),
                options)
            .map((res: Response) => {
                localStorage.setItem(LAST_LOGIN_TIME_IN_SECONDS_STORAGE_KEY, `${moment().toISOString()}`);
                return res.json();
            })
            .catch(this.onError);
    }

    logout(): Observable<any> {
        var headers: Headers = new Headers({
            'Content-Type': 'application/json'
        });
        var options: RequestOptions = new RequestOptions({ headers: headers });

        return this.http.post(
                URL.API_PREFIX + URL.Authentication,
                JSON.stringify({
                    requestType: 'LOGOUT'
                }), options)
            .map((res: Response) => {
                localStorage.removeItem(LAST_LOGIN_TIME_IN_SECONDS_STORAGE_KEY);
                return res.json();
            })
            .catch(this.onError);
    }

    private isCookieValid(): Observable<boolean> {
        var lastLogInTime = moment(localStorage.getItem(LAST_LOGIN_TIME_IN_SECONDS_STORAGE_KEY));
        if (!lastLogInTime || moment().diff(lastLogInTime, 'seconds') >= MAX_COOKIE_AGE_IN_SECONDS) {
            return Observable.of(false);
        }
        return Observable.of(true);
    }

    private onError(error: Response) {
        return Observable.throw(error.json() || 'Server error');
    }
}
