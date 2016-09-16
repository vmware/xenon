import { Injectable } from '@angular/core';
import { Http, Headers, RequestOptions, Response } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import * as _ from 'lodash';

import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import 'rxjs/add/observable/throw';
import 'rxjs/add/observable/forkJoin';

import { URL } from '../enums/url';
import { Node, ServiceDocument, ServiceDocumentQueryResult } from '../interfaces/index';

import { NodeSelectorService } from './node-selector.service';
import { LogService } from '../../core/index';

@Injectable()
export class BaseService {
    /**
     * The id of the node that hosts the current application.
     */
    protected _hostNodeId: string;

    /**
     * The id of the node whose information is being displayed in the views (not the node selector).
     */
    protected _selectedNodeId: string;

    constructor (
        protected _http: Http,
        protected _nodeSelectorService: NodeSelectorService,
        protected _logService: LogService) {
            this._nodeSelectorService.getSelectedNode().subscribe(
                (node: Node) => {
                    this._selectedNodeId = node ? node.id : '';
                },
                (error) => {
                    this._logService.error(`Failed to retrieve selected node: ${error}`);
                });

            this._nodeSelectorService.getHostNode().subscribe(
                (node: Node) => {
                    this._hostNodeId = node ? node.id : '';
                },
                (error) => {
                    this._logService.error(`Failed to retrieve host node: ${error}`);
                });
        }

    getDocumentLinks<T extends ServiceDocumentQueryResult>(targetLink: string,
            autoForward: boolean = true): Observable<string[]> {
        var link: string = targetLink;

        if (autoForward && this._selectedNodeId !== this._hostNodeId) {
            link = this._getForwardingLink(targetLink);
        }

        return this._http.get(URL.API_PREFIX + link)
            .map((res: Response) => {
                return <string[]> (res.json() as T).documentLinks;
            })
            .catch(this._onError);
    }

    getDocument<T extends ServiceDocument>(targetLink: string,
            autoForward: boolean = true): Observable<T> {
        return this._getDocument<T>(targetLink, autoForward);
    }

    getDocumentConfig<T extends ServiceDocument>(targetLink: string,
            autoForward: boolean = true): Observable<T> {
        return this._getDocument<T>(targetLink, autoForward, URL.CONFIG_SUFFIX);
    }

    getDocumentStats<T extends ServiceDocument>(targetLink: string,
            autoForward: boolean = true): Observable<T> {
        return this._getDocument<T>(targetLink, autoForward, URL.STATS_SUFFIX);
    }

    getDocuments<T extends ServiceDocument>(targetLinks: string[],
            autoForward: boolean = true): Observable<T[]> {
        return Observable.forkJoin(
                _.map(targetLinks, (targetLink: string) => {
                    var link: string = targetLink;

                    if (autoForward && this._selectedNodeId !== this._hostNodeId) {
                        link = this._getForwardingLink(targetLink);
                    }

                    return this._http.get(URL.API_PREFIX + link).map((res: Response) => {
                        var response: T = res.json();
                        if (_.isUndefined(response.documentSelfLink)) {
                            response.documentSelfLink = targetLink;
                        }
                        return response;
                    });
                })
            )
            .catch(this._onError);
    }

    post<T extends ServiceDocument>(targetLink: string, body: any,
            autoForward: boolean = true): Observable<T> {
        var headers = new Headers({ 'Content-Type': 'application/json' });
        var options = new RequestOptions({ headers: headers });

        var link: string = targetLink;

        if (autoForward && this._selectedNodeId !== this._hostNodeId) {
            link = this._getForwardingLink(targetLink);
        }

        return this._http.post(
                URL.API_PREFIX + link,
                _.isString(body) ? body : JSON.stringify(body),
                options)
            .map((res: Response) => {
                return <T> res.json();
            })
            .catch(this._onError);
    }

    patch<T extends ServiceDocument>(targetLink: string, body: any,
            autoForward: boolean = true): Observable<T> {
        var headers = new Headers({ 'Content-Type': 'application/json' });
        var options = new RequestOptions({ headers: headers });

        var link: string = targetLink;

        if (autoForward && this._selectedNodeId !== this._hostNodeId) {
            link = this._getForwardingLink(targetLink);
        }

        return this._http.patch(
                URL.API_PREFIX + link,
                _.isString(body) ? body : JSON.stringify(body),
                options)
            .map((res: Response) => {
                return <T> res.json();
            })
            .catch(this._onError);
    }

    put<T extends ServiceDocument>(targetLink: string, body: any,
            autoForward: boolean = true): Observable<T> {
        var headers = new Headers({ 'Content-Type': 'application/json' });
        var options = new RequestOptions({ headers: headers });

        var link: string = targetLink;

        if (autoForward && this._selectedNodeId !== this._hostNodeId) {
            link = this._getForwardingLink(targetLink);
        }

        return this._http.put(
                URL.API_PREFIX + link,
                _.isString(body) ? body : JSON.stringify(body),
                options)
            .map((res: Response) => {
                return <T> res.json();
            })
            .catch(this._onError);
    }

    delete<T extends ServiceDocument>(targetLink: string,
            autoForward: boolean = true): Observable<T> {
        var link: string = targetLink;

        if (autoForward && this._selectedNodeId !== this._hostNodeId) {
            link = this._getForwardingLink(targetLink);
        }

        return this._http.delete(URL.API_PREFIX + link)
            .map((res: Response) => {
                return <T> res.json();
            })
            .catch(this._onError);
    }

    protected _getDocument<T extends ServiceDocument>(targetLink: string,
            autoForward: boolean = true, suffix: string = ''): Observable<T> {
        var link: string = targetLink + suffix;

        if (autoForward && this._selectedNodeId !== this._hostNodeId) {
            link = this._getForwardingLink(link);
        }

        return this._http.get(URL.API_PREFIX + link)
            .map((res: Response) => {
                return <T> res.json();
            })
            .catch(this._onError);
    }

    protected _getForwardingLink(targetLink: string, query: string = ''): string {
        return `${URL.FORWARDING_PATH}?peer=${this._selectedNodeId}&path=${targetLink}&query=${query}&target=PEER_ID`;
    }

    protected _onError(error: Response) {
        return Observable.throw(error.json() || 'Server error');
    }
}
