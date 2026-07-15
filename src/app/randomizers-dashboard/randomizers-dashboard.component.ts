import { Component } from '@angular/core';
import { RouterLink } from '@angular/router';
import { TranslocoDirective } from '@jsverse/transloco';
import { LanguageSelectorComponent } from '../language-selector/language-selector.component';
import { MaterialModule } from '../material/material.module';
import { PageFooterComponent } from '../page-footer/page-footer.component';

@Component({
  selector: 'app-randomizers-dashboard',
  standalone: true,
  imports: [
    RouterLink,
    TranslocoDirective,
    LanguageSelectorComponent,
    MaterialModule,
    PageFooterComponent,
  ],
  templateUrl: './randomizers-dashboard.component.html',
  styleUrl: './randomizers-dashboard.component.scss',
})
export class RandomizersDashboardComponent {}
