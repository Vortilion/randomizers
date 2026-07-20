import { Component, inject } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import {
  MatDialogActions,
  MatDialogClose,
  MatDialogContent,
  MatDialogRef,
  MatDialogTitle,
} from '@angular/material/dialog';
import { TranslocoDirective } from '@jsverse/transloco';

@Component({
  selector: 'app-variant-warning-dialog',
  imports: [
    MatButtonModule,
    MatDialogActions,
    MatDialogClose,
    MatDialogTitle,
    MatDialogContent,
    TranslocoDirective,
  ],
  templateUrl: './variant-warning-dialog.component.html',
})
export class VariantWarningDialogComponent {
  readonly dialogRef = inject(MatDialogRef<VariantWarningDialogComponent>);
}
