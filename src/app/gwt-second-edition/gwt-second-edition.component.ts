import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { Component, signal, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import type { MatSelectChange } from '@angular/material/select';
import type { MatSlideToggleChange } from '@angular/material/slide-toggle';
import { TranslocoDirective } from '@jsverse/transloco';
import { map } from 'rxjs/operators';
import { MaterialModule } from '../material/material.module';
import type { PlayerCountOption } from '../models/player-count-option.model';
import type { Tile } from '../models/tile.model';
import { PageFooterComponent } from '../page-footer/page-footer.component';
import { PageHeaderComponent } from '../page-header/page-header.component';
import { LocalStorageService } from '../shared/local-storage.service';
import { GwtSecondEditionConfigService } from './gwt-second-edition-config.service';
import { VariantWarningDialogComponent } from './variant-warning-dialog.component';

@Component({
  selector: 'app-gwt-second-edition',
  imports: [
    FormsModule,
    MaterialModule,
    PageHeaderComponent,
    PageFooterComponent,
    TranslocoDirective,
  ],
  templateUrl: './gwt-second-edition.component.html',
  styleUrl: './gwt-second-edition.component.scss',
})
export class GwtSecondEditionComponent {
  readonly dialog = inject(MatDialog);
  private applicationConfigService = inject(GwtSecondEditionConfigService);
  private responsive = inject(BreakpointObserver);
  private storage = inject(LocalStorageService);

  randomNeutralBuildings = signal<Tile[]>([]);
  randomPlayerBuildings = signal<Tile[]>([]);
  randomStationMasters = signal<Tile[]>([]);
  playerCountList = signal<PlayerCountOption[]>([
    { label: '2', value: 2 },
    { label: '3', value: 3 },
    { label: '4', value: 4 },
  ]);

  isXSmall = toSignal(
    this.responsive.observe(Breakpoints.XSmall).pipe(
      map((result) => result.matches)
    ),
    { initialValue: false }
  );

  isMax1280 = toSignal(
    this.responsive.observe('(max-width: 1280px)').pipe(
      map((result) => result.matches)
    ),
    { initialValue: false }
  );

  playerCount = this.applicationConfigService.playerCount;
  useSimmental = signal<boolean>(false);
  useBrahman = signal<boolean>(false);
  useRailsToTheNorth = this.applicationConfigService.useRailsToTheNorth;

  constructor() {
    const playerCount = this.storage.get<number>('gwt2-playerCount');
    if (typeof playerCount === 'number') {
      this.applicationConfigService.setPlayerCount(playerCount);
    } else {
      this.storage.set('gwt2-playerCount', 2);
    }

    const useSimmental = this.storage.get<boolean>('gwt2-useSimmental');
    if (typeof useSimmental === 'boolean') {
      this.useSimmental.set(useSimmental);
      this.applicationConfigService.setUseVariant({
        name: 'useSimmental',
        checked: useSimmental,
      });
    } else {
      this.storage.set('gwt2-useSimmental', false);
    }

    const useBrahman = this.storage.get<boolean>('gwt2-useBrahman');
    if (typeof useBrahman === 'boolean') {
      this.useBrahman.set(useBrahman);
      this.applicationConfigService.setUseVariant({
        name: 'useBrahman',
        checked: useBrahman,
      });
    } else {
      this.storage.set('gwt2-useBrahman', false);
    }

    const useRailsToTheNorth = this.storage.get<boolean>('gwt2-useRailsToTheNorth');
    if (typeof useRailsToTheNorth === 'boolean') {
      this.applicationConfigService.setUseRailsToTheNorth(useRailsToTheNorth);
    } else {
      this.storage.set('gwt2-useRailsToTheNorth', false);
    }

    this.randomizeSetup();
  }

  openDialog() {
    return this.dialog.open(VariantWarningDialogComponent);
  }

  onPlayerCountChange(event: MatSelectChange): void {
    const playerCount = Number(event.value);
    this.storage.set('gwt2-playerCount', playerCount);
    this.applicationConfigService.setPlayerCount(playerCount);
  }

  resetVariants(): void {
    const dialogRef = this.openDialog();

    dialogRef.afterClosed().subscribe(() => {
      this.storage.set('gwt2-useSimmental', false);
      this.useSimmental.set(false);

      this.applicationConfigService.setUseVariant({
        name: 'useSimmental',
        checked: false,
      });

      this.storage.set('gwt2-useBrahman', false);
      this.useBrahman.set(false);

      this.applicationConfigService.setUseVariant({
        name: 'useBrahman',
        checked: false,
      });
    });
  }

  onVariantChange(name: string, event: MatSlideToggleChange): void {
    if (
      (this.useBrahman() && name === 'useSimmental' && event.checked) ||
      (this.useSimmental() && name === 'useBrahman' && event.checked)
    ) {
      this.resetVariants();
    } else {
      this.storage.set(`gwt2-${event.source.name}`, event.checked);
      if (name === 'useSimmental') {
        this.useSimmental.set(event.checked);
      } else if (name === 'useBrahman') {
        this.useBrahman.set(event.checked);
      }
      this.applicationConfigService.setUseVariant({
        name,
        checked: event.checked,
      });
    }
  }

  onExpansionChange(event: MatSlideToggleChange): void {
    this.storage.set('gwt2-useRailsToTheNorth', event.checked);
    this.applicationConfigService.setUseRailsToTheNorth(event.checked);
  }

  randomizeSetup(): void {
    this.randomNeutralBuildings.set(
      this.applicationConfigService.getRandomNeutralBuildingOrder(),
    );

    this.randomStationMasters.set(
      this.applicationConfigService.getRandomStationMasters(),
    );

    this.randomPlayerBuildings.set(
      this.applicationConfigService.getRandomPlayerBuildings(),
    );
  }
}
