import {t} from '../../test/index';
import {AppConfig} from './app-config';

export function main() {
    t.describe('app: AppConfig', () => {

        t.it('SUPPORTED_LANGUAGES', () => {
            t.e(AppConfig.SUPPORTED_LANGUAGES.length).toBe(1);
            t.e(AppConfig.SUPPORTED_LANGUAGES[0].code).toBe('en');
        });
    });
}
