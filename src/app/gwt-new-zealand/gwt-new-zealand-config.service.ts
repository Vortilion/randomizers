import { Injectable, signal } from '@angular/core';
import type { Tile } from '../models/tile.model';

@Injectable({
  providedIn: 'root',
})
export class GwtNewZealandConfigService {
  playerCount = signal<number>(2);

  setPlayerCount(count: number): void {
    this.playerCount.set(count);
  }

  neutralBuildings: Tile[] = [
    { title: 'A', sides: [{ title: 'front' }] },
    { title: 'B', sides: [{ title: 'front' }] },
    { title: 'C', sides: [{ title: 'front' }] },
    { title: 'D', sides: [{ title: 'front' }] },
    { title: 'E', sides: [{ title: 'front' }] },
    { title: 'F', sides: [{ title: 'front' }] },
    { title: 'G', sides: [{ title: 'front' }] },
    { title: 'H', sides: [{ title: 'front' }] },
  ];

  playerBuildings: Tile[] = [
    { title: '1', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '2', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '3', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '4', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '5', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '6', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '7', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '8', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '9', sides: [{ title: 'a' }, { title: 'b' }] },
    { title: '10', sides: [{ title: 'a' }, { title: 'b' }] },
  ];

  harborMasters: Tile[] = [
    { title: '1', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-01.png' }] },
    { title: '2', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-02.png' }] },
    { title: '3', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-03.png' }] },
    { title: '4', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-04.png' }] },
    { title: '5', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-05.png' }] },
    { title: '6', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-06.png' }] },
    { title: '7', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-07.png' }] },
    { title: '8', sides: [{ title: 'front', image: 'img/gwt-new-zealand/harbormaster-08.png' }] },
  ];

  deckBuildingModules: string[] = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'];

  getRandomNeutralBuildingOrder(): Tile[] {
    return this.shuffleArray(this.neutralBuildings);
  }

  getRandomHarborMasters(): Tile[] {
    const selection: Tile[] = [];
    const shuffledHarborMasters = this.shuffleArray(this.harborMasters);

    for (let index = 0; index < 5; index += 1) {
      selection.push(shuffledHarborMasters.pop() as Tile);
    }

    return selection;
  }

  getRandomDeckbuildingModules(): string[] {
    const selection: string[] = [];
    const shuffledDeckbuildingModules = this.shuffleArray(this.deckBuildingModules);

    for (let index = 0; index < 4; index += 1) {
      selection.push(shuffledDeckbuildingModules.pop() as string);
    }

    return selection;
  }

  getRandomPlayerBuildings(): Tile[] {
    const playerBuildings = JSON.parse(JSON.stringify(this.playerBuildings)) as Tile[];

    playerBuildings.forEach((playerBuilding) => {
      playerBuilding.sides.splice(
        Math.floor(Math.random() * playerBuilding.sides.length),
        1,
      );
    });

    return playerBuildings;
  }

  private shuffleArray<T>(inArray: T[]): T[] {
    const returnArray = inArray.slice();

    for (let index = returnArray.length - 1; index > 0; index -= 1) {
      const swapIndex = Math.floor(Math.random() * (index + 1));
      const current = returnArray[index];
      returnArray[index] = returnArray[swapIndex];
      returnArray[swapIndex] = current;
    }

    return returnArray;
  }
}
