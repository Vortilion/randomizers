import { Routes } from '@angular/router';
import { ArgentinaComponent } from './argentina/argentina.component';
import { RandomizersDashboardComponent } from './randomizers-dashboard/randomizers-dashboard.component';
import { SecondEditionComponent } from './second-edition/second-edition.component';

export const routes: Routes = [
  {
    path: 'randomizers',
    children: [
      {
        path: '',
        component: RandomizersDashboardComponent,
      },
      {
        path: 'argentina',
        component: ArgentinaComponent,
      },
      {
        path: '2nd-edition',
        component: SecondEditionComponent,
      },
    ],
  },
  { path: '', redirectTo: 'randomizers', pathMatch: 'full' },
  { path: '**', redirectTo: 'randomizers', pathMatch: 'full' },
];
