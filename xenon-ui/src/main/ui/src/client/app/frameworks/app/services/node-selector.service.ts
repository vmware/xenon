import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { Node } from '../interfaces/index';

@Injectable()
export class NodeSelectorService {
    /**
     * The subject (both Observable and Observer) that monitors the host node.
     */
    private _hostNodeSubject: Subject<Node> = new Subject<Node>();

    /**
     * The subject (both Observable and Observer) that monitors the selected node.
     */
    private _selectedNodeSubject: Subject<Node> = new Subject<Node>();

    /**
     * The subject (both Observable and Observer) that monitors whether the node
     * selector is active
     */
    private _nodeSelectorActiveSubject: Subject<boolean> = new Subject<boolean>();

    setHostNode(node: Node): void {
        this._hostNodeSubject.next(node);
    }

    getHostNode(): Observable<Node> {
        return this._hostNodeSubject as Observable<Node>;
    }

    setSelectedNode(node: Node): void {
        this._selectedNodeSubject.next(node);
    }

    getSelectedNode(): Observable<Node> {
        return this._selectedNodeSubject as Observable<Node>;
    }

    setNodeSelectorActive(isActive: boolean): void {
        this._nodeSelectorActiveSubject.next(isActive);
    }

    getNodeSelectorActive(): Observable<boolean> {
        return this._nodeSelectorActiveSubject as Observable<boolean>;
    }
}
