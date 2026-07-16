import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { MatSidenav } from '@angular/material/sidenav';
import { Component, input, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { TranslocoPipe } from '@jsverse/transloco';
import { toSignal } from '@angular/core/rxjs-interop';
import { map } from 'rxjs/operators';
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
export class PageHeaderComponent {
  private responsive = inject(BreakpointObserver);

  sidebarHandle = input<MatSidenav | undefined>();
  dashboardRoute = input<string>();
  titlePrefix = input<string>('');

  // Unwrap signals for template use
  get sidebarHandleRef(): MatSidenav | undefined {
    return this.sidebarHandle();
  }

  get dashboardRouteRef(): string | undefined {
    return this.dashboardRoute();
  }

  get titlePrefixRef(): string {
    return this.titlePrefix();
  }

  isXSmall = toSignal(
    this.responsive.observe(Breakpoints.XSmall).pipe(
      map((result) => result.matches)
    ),
    { initialValue: false }
  );
}
