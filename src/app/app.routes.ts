import { Routes } from '@angular/router';
import { ArgentinaComponent } from './argentina/argentina.component';
import { RandomizersDashboardComponent } from './randomizers-dashboard/randomizers-dashboard.component';
import { SecondEditionComponent } from './second-edition/second-edition.component';

export const routes: Routes = [
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
  { path: '**', redirectTo: '', pathMatch: 'full' },
];
