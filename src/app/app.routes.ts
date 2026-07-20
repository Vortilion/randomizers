import { Routes } from '@angular/router';
import { GwtArgentinaComponent } from './gwt-argentina/gwt-argentina.component';
import { RandomizersDashboardComponent } from './randomizers-dashboard/randomizers-dashboard.component';
import { GwtSecondEditionComponent } from './gwt-second-edition/gwt-second-edition.component';
import { GwtNewZealandComponent } from './gwt-new-zealand/gwt-new-zealand.component';

export const routes: Routes = [
  {
    path: '',
    component: RandomizersDashboardComponent,
  },
  {
    path: 'gwt-argentina',
    component: GwtArgentinaComponent,
  },
  {
    path: 'gwt-2nd-edition',
    component: GwtSecondEditionComponent,
  },
  {
    path: 'gwt-new-zealand',
    component: GwtNewZealandComponent,
  },
  { path: '**', redirectTo: '', pathMatch: 'full' },
];
