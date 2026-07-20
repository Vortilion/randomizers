import { Component } from '@angular/core';
import { TranslocoPipe } from '@jsverse/transloco';
import { MaterialModule } from '../material/material.module';

@Component({
  selector: 'app-page-footer',
  imports: [MaterialModule, TranslocoPipe],
  templateUrl: './page-footer.component.html',
  styleUrls: ['./page-footer.component.scss'],
})
export class PageFooterComponent {}
