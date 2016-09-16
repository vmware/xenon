
import { BaseComponent } from '../../frameworks/core/index';

import { AboutSubNavComponent } from './index';

@BaseComponent({
  selector: 'xe-about',
  moduleId: module.id,
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.css'],
  directives: [AboutSubNavComponent]
})

export class AboutComponent {}
