import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { Node } from '../interfaces/index';

@Injectable()
export class NodeSelectorService {
    /**
     * The subject (both Observable and Observer) that monitors the host node.
     */
    private hostNodeSubject: Subject<Node> = new Subject<Node>();

    /**
     * The subject (both Observable and Observer) that monitors the selected node.
     */
    private selectedNodeSubject: Subject<Node> = new Subject<Node>();

    /**
     * The subject (both Observable and Observer) that monitors whether the node
     * selector is active
     */
    private nodeSelectorActiveSubject: Subject<boolean> = new Subject<boolean>();

    setHostNode(node: Node): void {
        this.hostNodeSubject.next(node);
    }

    getHostNode(): Observable<Node> {
        return this.hostNodeSubject as Observable<Node>;
    }

    setSelectedNode(node: Node): void {
        this.selectedNodeSubject.next(node);
    }

    getSelectedNode(): Observable<Node> {
        return this.selectedNodeSubject as Observable<Node>;
    }

    setNodeSelectorActive(isActive: boolean): void {
        this.nodeSelectorActiveSubject.next(isActive);
    }

    getNodeSelectorActive(): Observable<boolean> {
        return this.nodeSelectorActiveSubject as Observable<boolean>;
    }
}
