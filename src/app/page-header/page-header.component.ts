import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { MatSidenav } from '@angular/material/sidenav';
import { Component, Input, OnInit, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { TranslocoPipe } from '@jsverse/transloco';
import { LanguageSelectorComponent } from '../language-selector/language-selector.component';
import { MaterialModule } from '../material/material.module';

@Component({
  selector: 'app-page-header',
  standalone: true,
  imports: [
    MaterialModule,
    LanguageSelectorComponent,
    TranslocoPipe,
    RouterLink,
  ],
  templateUrl: './page-header.component.html',
  styleUrls: ['./page-header.component.scss'],
})
export class PageHeaderComponent implements OnInit {
  private responsive = inject(BreakpointObserver);

  @Input() sidebarHandle!: MatSidenav;
  @Input() dashboardRoute?: string;
  @Input() titlePrefix = '';
  isXSmall!: boolean;

  ngOnInit(): void {
    this.responsive.observe(Breakpoints.XSmall).subscribe((result) => {
      if (result.matches) {
        this.isXSmall = true;
      } else {
        this.isXSmall = false;
      }
    });
  }
}
