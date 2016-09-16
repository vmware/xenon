// angular
import { OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';

// app
import { BaseComponent } from '../../frameworks/core/index';

import { StarCanvasComponent } from './star-canvas.component';

import { AuthenticationService } from '../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-login',
    moduleId: module.id,
    templateUrl: './login.component.html',
    styleUrls: ['./login.component.css'],
    directives: [StarCanvasComponent]
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
    private _loginServiceSubscription: Subscription;

    constructor(
        private _authenticationService: AuthenticationService,
        private _router: Router) {}

    ngOnDestroy(): void {
        if (!_.isUndefined(this._loginServiceSubscription)) {
            this._loginServiceSubscription.unsubscribe();
        }
    }

    onLogin(): void {
        if (!this._validate()) {
            return;
        }

        this.isLoading = true;

        this._loginServiceSubscription =
            this._authenticationService.login(this.username, this.password).subscribe(
                (data) => {
                    this.validationErrorMessage = '';
                    this.isLoading = false;

                    var redirectUrl = this._authenticationService.redirectUrl ?
                        this._authenticationService.redirectUrl : '/main/dashboard';

                    this._router.navigate([redirectUrl]);
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

    private _validate(): boolean {
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
