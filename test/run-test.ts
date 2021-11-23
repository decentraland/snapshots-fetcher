import * as path from 'path'
import { checkFileExists } from '../src/utils'
import { createFetchComponent } from './test-component'

const productiveServers = [
  'https://peer.decentraland.org', // DCL
  'https://peer-ec1.decentraland.org', // DCL - US East
  'https://peer-wc1.decentraland.org', // DCL - US West
  'https://peer-eu1.decentraland.org', // DCL - EU
  'https://peer-ap1.decentraland.org', // DCL - AP1
  'https://interconnected.online', // Esteban
  'https://peer.decentral.games', // Baus
  'https://peer.melonwave.com', // Ari
  'https://peer.kyllian.me', // Kyllian
  'https://peer.uadevops.com', // SFox
  'https://peer.dclnodes.io', // DSM
]

const downloadsFolder = path.resolve('downloads')

downloadEntities({
  catalystServers: productiveServers,
  async deployAction(entity) {
    console.dir(entity)
    console.log('Deploying local entity ' + entity.entityId)
  },
  concurrency: 10,
  jobTimeout: 30000,
  async isEntityPresentLocally(entityId) {
    return checkFileExists(path.join(`downloads`, entityId))
  },
  contentFolder: downloadsFolder,
  components: {
    fetcher: createFetchComponent(),
  },
  entityTypes: ['wearables', 'scenes'],
}).catch((err) => {
  console.log('ERROR', err)
})
