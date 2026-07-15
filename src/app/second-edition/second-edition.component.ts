import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { Component, OnInit, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import type { MatSelectChange } from '@angular/material/select';
import type { MatSlideToggleChange } from '@angular/material/slide-toggle';
import { TranslocoDirective } from '@jsverse/transloco';
import { MaterialModule } from '../material/material.module';
import type { PlayerCountOption } from '../models/player-count-option.model';
import type { Tile } from '../models/tile.model';
import { PageFooterComponent } from '../page-footer/page-footer.component';
import { PageHeaderComponent } from '../page-header/page-header.component';
import { LocalStorageService } from '../shared/local-storage.service';
import { SecondEditionConfigService } from './second-edition-config.service';
import { VariantWarningDialogComponent } from './variant-warning-dialog.component';

@Component({
  selector: 'app-second-edition',
  standalone: true,
  imports: [
    FormsModule,
    MaterialModule,
    PageHeaderComponent,
    PageFooterComponent,
    TranslocoDirective,
  ],
  templateUrl: './second-edition.component.html',
  styleUrl: './second-edition.component.scss',
})
export class SecondEditionComponent implements OnInit {
  readonly dialog = inject(MatDialog);
  private applicationConfigService = inject(SecondEditionConfigService);
  private responsive = inject(BreakpointObserver);
  private storage = inject(LocalStorageService);

  randomNeutralBuildings!: Tile[];
  randomPlayerBuildings!: Tile[];
  randomStationMasters!: Tile[];
  playerCount!: number;
  playerCountList!: PlayerCountOption[];
  isXSmall!: boolean;
  isMax1280!: boolean;
  useSimmental!: boolean;
  useBrahman!: boolean;
  useRailsToTheNorth!: boolean;

  ngOnInit(): void {
    this.playerCount = 2;
    this.playerCountList = [
      {
        label: '2',
        value: 2,
      },
      {
        label: '3',
        value: 3,
      },
      {
        label: '4',
        value: 4,
      },
    ];

    this.useSimmental = false;
    this.useRailsToTheNorth = false;
    this.useBrahman = false;

    this.responsive.observe(Breakpoints.XSmall).subscribe((result) => {
      this.isXSmall = result.matches;
    });

    this.responsive.observe('(max-width: 1280px)').subscribe((result) => {
      this.isMax1280 = result.matches;
    });

    const playerCount = this.storage.get<number>('gwt2-playerCount');
    if (typeof playerCount === 'number') {
      this.emitPlayerCount(playerCount);
    } else {
      this.storage.set('gwt2-playerCount', 2);
    }

    const useSimmental = this.storage.get<boolean>('gwt2-useSimmental');
    if (typeof useSimmental === 'boolean') {
      this.applicationConfigService.useVariant.emit({
        name: 'useSimmental',
        checked: useSimmental,
      });
    } else {
      this.storage.set('gwt2-useSimmental', false);
    }

    const useBrahman = this.storage.get<boolean>('gwt2-useBrahman');
    if (typeof useBrahman === 'boolean') {
      this.applicationConfigService.useVariant.emit({
        name: 'useBrahman',
        checked: useBrahman,
      });
    } else {
      this.storage.set('gwt2-useBrahman', false);
    }

    const useRailsToTheNorth = this.storage.get<boolean>('gwt2-useRailsToTheNorth');
    if (typeof useRailsToTheNorth === 'boolean') {
      this.applicationConfigService.useRailsToTheNorth.emit(useRailsToTheNorth);
    } else {
      this.storage.set('gwt2-useRailsToTheNorth', false);
    }

    this.applicationConfigService.playerCount.subscribe((playerCountValue: number) => {
      this.playerCount = playerCountValue;
    });

    this.applicationConfigService.useVariant.subscribe((event) => {
      if (event.name === 'useSimmental') {
        this.useSimmental = event.checked;
      } else if (event.name === 'useBrahman') {
        this.useBrahman = event.checked;
      }
    });

    this.applicationConfigService.useRailsToTheNorth.subscribe((enabled: boolean) => {
      this.useRailsToTheNorth = enabled;
    });

    this.randomizeSetup();
  }

  openDialog() {
    return this.dialog.open(VariantWarningDialogComponent);
  }

  emitPlayerCount(playerCount: number) {
    this.applicationConfigService.playerCount.emit(playerCount);
  }

  onPlayerCountChange(event: MatSelectChange) {
    this.storage.set('gwt2-playerCount', event.value);
    this.emitPlayerCount(Number(event.value));
  }

  resetVariants() {
    const dialogRef = this.openDialog();

    dialogRef.afterClosed().subscribe(() => {
      this.storage.set('gwt2-useSimmental', false);
      this.useSimmental = false;

      this.applicationConfigService.useVariant.emit({
        name: 'useSimmental',
        checked: false,
      });

      this.storage.set('gwt2-useBrahman', false);
      this.useBrahman = false;

      this.applicationConfigService.useVariant.emit({
        name: 'useBrahman',
        checked: false,
      });
    });
  }

  onVariantChange(name: string, event: MatSlideToggleChange) {
    if (
      (this.useBrahman && name === 'useSimmental' && event.checked) ||
      (this.useSimmental && name === 'useBrahman' && event.checked)
    ) {
      this.resetVariants();
    } else {
      this.storage.set(`gwt2-${event.source.name}`, event.checked);
      this.applicationConfigService.useVariant.emit({
        name,
        checked: event.checked,
      });
    }
  }

  onExpansionChange(event: MatSlideToggleChange) {
    this.storage.set('gwt2-useRailsToTheNorth', event.checked);
    this.applicationConfigService.useRailsToTheNorth.emit(event.checked);
  }

  randomizeSetup() {
    this.randomNeutralBuildings =
      this.applicationConfigService.getRandomNeutralBuildingOrder();

    this.randomStationMasters =
      this.applicationConfigService.getRandomStationMasters();

    this.randomPlayerBuildings =
      this.applicationConfigService.getRandomPlayerBuildings();
  }
}
