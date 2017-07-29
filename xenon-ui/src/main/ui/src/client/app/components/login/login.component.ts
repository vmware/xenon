// angular
import { ChangeDetectionStrategy, Component, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { AuthenticationService } from '../../modules/app/services/index';

@Component({
    selector: 'xe-login',
    moduleId: module.id,
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.css'],
    changeDetection: ChangeDetectionStrategy.OnPush
})

export class LoginComponent implements OnDestroy {
    /**
     * Username input value
     */
    username: string;

    /**
     * Password input value
     */
    password: string;

    /**
     * Message for valitation error. At any given moment there's only one error
     * message.
     */
    validationErrorMessage: string;

    /**
     * Is the page loading or making requests to the server.
     */
    isLoading: boolean = false;

    /**
     * Subscriptions to services.
     */
    private loginServiceSubscription: Subscription;

    constructor(
        private authenticationService: AuthenticationService,
        private router: Router) {}

    ngOnDestroy(): void {
        if (!_.isUndefined(this.loginServiceSubscription)) {
            this.loginServiceSubscription.unsubscribe();
        }
    }

    onLogin(): void {
        if (!this.validate()) {
            return;
        }

        this.isLoading = true;

        this.loginServiceSubscription =
            this.authenticationService.login(this.username, this.password).subscribe(
                (data) => {
                    this.validationErrorMessage = '';
                    this.isLoading = false;

                    var redirectUrl = this.authenticationService.redirectUrl ?
                        this.authenticationService.redirectUrl : '/main/dashboard';

                    this.router.navigate([redirectUrl]);
                },
                (error) => {
                    if (error.statusCode === 401 || error.statusCode === 403) {
                        this.validationErrorMessage = 'Invalid user name or password';
                    } else {
                        this.validationErrorMessage = 'Server error';
                    }
                    this.isLoading = false;
                });
    }

    private validate(): boolean {
        if (!this.username) {
            this.validationErrorMessage = 'Username can not be empty';
            return false;
        }

        if (!this.password) {
            this.validationErrorMessage = 'Password can not be empty';
            return false;
        }

        return true;
    }
}
