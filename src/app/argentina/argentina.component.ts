import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { Component, signal, inject } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { MatSelectChange } from '@angular/material/select';
import { TranslocoDirective } from '@jsverse/transloco';
import { map } from 'rxjs/operators';
import type { PlayerCountOption } from '../models/player-count-option.model';
import type { Tile } from '../models/tile.model';
import { ArgentinaConfigService } from './argentina-config.service';
import { MaterialModule } from '../material/material.module';
import { PageFooterComponent } from '../page-footer/page-footer.component';
import { PageHeaderComponent } from '../page-header/page-header.component';
import { LocalStorageService } from '../shared/local-storage.service';

@Component({
  selector: 'app-argentina',
  standalone: true,
  imports: [
    MaterialModule,
    PageHeaderComponent,
    PageFooterComponent,
    TranslocoDirective,
  ],
  templateUrl: './argentina.component.html',
  styleUrls: ['./argentina.component.scss'],
})
export class ArgentinaComponent {
  private applicationConfigService = inject(ArgentinaConfigService);
  private responsive = inject(BreakpointObserver);
  private storageService = inject(LocalStorageService);

  randomNeutralBuildings = signal<Tile[]>([]);
  randomPlayerBuildings = signal<Tile[]>([]);
  randomStationMasters = signal<Tile[]>([]);
  randomCities = signal<Tile[]>([]);
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

  constructor() {
    const playerCountValue = this.storageService.getNumber('rar-playerCount');
    if (playerCountValue !== null) {
      this.applicationConfigService.setPlayerCount(playerCountValue);
    } else {
      this.storageService.setNumber('rar-playerCount', 2);
    }

    this.randomizeSetup();
  }

  onPlayerCountChange(event: MatSelectChange): void {
    const playerCount = Number(event.value);
    this.storageService.setNumber('rar-playerCount', playerCount);
    this.applicationConfigService.setPlayerCount(playerCount);
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

    this.randomCities.set(this.applicationConfigService.getRandomCities());
  }
}
