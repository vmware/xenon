import { Notification } from './notification';

/**
 * Context for rendering/controlling modals in the view.
 */
export interface ModalContext {
    // isOpened: boolean;
    name: string;
    data: {[key: string]: any};
    notifications?: Notification[];
    condition?: any;
}
