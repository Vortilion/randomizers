import { BreakpointObserver, Breakpoints } from '@angular/cdk/layout';
import { Component, OnInit, inject } from '@angular/core';
import { MatSelectChange } from '@angular/material/select';
import { TranslocoDirective } from '@jsverse/transloco';
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
export class ArgentinaComponent implements OnInit {
  private applicationConfigService = inject(ArgentinaConfigService);
  private responsive = inject(BreakpointObserver);
  private storageService = inject(LocalStorageService);

  randomNeutralBuildings!: Tile[];
  randomPlayerBuildings!: Tile[];
  randomStationMasters!: Tile[];
  randomCities!: Tile[];
  playerCount!: number;
  playerCountList!: PlayerCountOption[];
  isXSmall!: boolean;
  isMax1280!: boolean;

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

    this.responsive.observe(Breakpoints.XSmall).subscribe((result) => {
      if (result.matches) {
        this.isXSmall = true;
      } else {
        this.isXSmall = false;
      }
    });

    this.responsive.observe('(max-width: 1280px)').subscribe((result) => {
      if (result.matches) {
        this.isMax1280 = true;
      } else {
        this.isMax1280 = false;
      }
    });

    const playerCount = this.storageService.getNumber('rar-playerCount');
    if (playerCount !== null) {
      this.emitPlayerCount(playerCount);
    } else {
      this.storageService.setNumber('rar-playerCount', 2);
    }

    this.applicationConfigService.playerCount.subscribe(
      (playerCountValue: number) => {
        this.playerCount = playerCountValue;
      },
    );

    this.randomizeSetup();
  }

  emitPlayerCount(playerCount: number) {
    this.applicationConfigService.playerCount.emit(playerCount);
  }

  onPlayerCountChange(event: MatSelectChange) {
    const playerCount = Number(event.value);
    this.storageService.setNumber('rar-playerCount', playerCount);
    this.emitPlayerCount(playerCount);
  }

  randomizeSetup() {
    this.randomNeutralBuildings =
      this.applicationConfigService.getRandomNeutralBuildingOrder();

    this.randomStationMasters =
      this.applicationConfigService.getRandomStationMasters();

    this.randomPlayerBuildings =
      this.applicationConfigService.getRandomPlayerBuildings();

    this.randomCities = this.applicationConfigService.getRandomCities();
  }
}
