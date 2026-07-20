import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { Component, signal, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { MatSelectChange } from '@angular/material/select';
import { TranslocoDirective } from '@jsverse/transloco';
import { map } from 'rxjs/operators';
import type { PlayerCountOption } from '../models/player-count-option.model';
import type { Tile } from '../models/tile.model';
import { GwtNewZealandConfigService } from './gwt-new-zealand-config.service';
import { MaterialModule } from '../material/material.module';
import { PageFooterComponent } from '../page-footer/page-footer.component';
import { PageHeaderComponent } from '../page-header/page-header.component';
import { LocalStorageService } from '../shared/local-storage.service';

@Component({
  selector: 'app-gwt-new-zealand',
  imports: [
    MaterialModule,
    PageHeaderComponent,
    PageFooterComponent,
    TranslocoDirective,
  ],
  templateUrl: './gwt-new-zealand.component.html',
  styleUrls: ['./gwt-new-zealand.component.scss'],
})
export class GwtNewZealandComponent {
  applicationConfigService = inject(GwtNewZealandConfigService);
  private responsive = inject(BreakpointObserver);
  private storageService = inject(LocalStorageService);

  randomNeutralBuildings = signal<Tile[]>([]);
  randomPlayerBuildings = signal<Tile[]>([]);
  randomHarborMasters = signal<Tile[]>([]);
  randomDeckbuildingModules = signal<string[]>([]);
  playerCountList = signal<PlayerCountOption[]>([
    { label: '2', value: 2 },
    { label: '3', value: 3 },
    { label: '4', value: 4 },
  ]);

  playerCount = this.applicationConfigService.playerCount;

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

  constructor() {
    const playerCountFromStorage = this.storageService.getNumber('gwt-nz-playerCount');
    if (playerCountFromStorage !== null) {
      this.applicationConfigService.setPlayerCount(playerCountFromStorage);
    } else {
      this.storageService.setNumber('gwt-nz-playerCount', 2);
    }

    this.randomizeSetup();
  }

  onPlayerCountChange(event: MatSelectChange): void {
    const playerCount = Number(event.value);
    this.storageService.setNumber('gwt-nz-playerCount', playerCount);
    this.applicationConfigService.setPlayerCount(playerCount);
  }

  randomizeSetup(): void {
    this.randomNeutralBuildings.set(
      this.applicationConfigService.getRandomNeutralBuildingOrder(),
    );

    this.randomPlayerBuildings.set(
      this.applicationConfigService.getRandomPlayerBuildings(),
    );

    this.randomHarborMasters.set(
      this.applicationConfigService.getRandomHarborMasters(),
    );

    this.randomDeckbuildingModules.set(
      this.applicationConfigService.getRandomDeckbuildingModules(),
    );
  }
}
