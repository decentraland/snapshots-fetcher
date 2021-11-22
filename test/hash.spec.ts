import { assertHash } from '../src/utils'

describe('hashing checks', () => {
  it('checks bafy', async () => {
    await assertHash(
      'test/fixtures/hashes/bafybeibdik2ihfpcdi7aaaguptwcoc5msav7uhn5hu54xlq2pdwkh5arzy',
      'bafybeibdik2ihfpcdi7aaaguptwcoc5msav7uhn5hu54xlq2pdwkh5arzy'
    )
  })

  it('checks Qm', async () => {
    await assertHash(
      'test/fixtures/hashes/QmSYpJEQLQc82USvtavzxEiBR57nyb5RdMzecBTR3Qg6qn',
      'QmSYpJEQLQc82USvtavzxEiBR57nyb5RdMzecBTR3Qg6qn'
    )
  })

  it('checks Qm failure', async () => {
    await expect(
      assertHash(
        'test/fixtures/hashes/QmSYpJEQLQc82USvtavzxEiBR57nyb5RdMzecBTR3Qg6qn',
        'QmSYpJEQLQc82USvtavzxEiBR57nyb5RdMzecBTR3QgAAA'
      )
    ).rejects.toThrow()
  })

  it('checks bafy failure', async () => {
    await expect(
      assertHash(
        'test/fixtures/hashes/bafybeibdik2ihfpcdi7aaaguptwcoc5msav7uhn5hu54xlq2pdwkh5arzy',
        'bafybeibdik2ihfpcdi7aaaguptwcoc5msav7uhn5hu54xlq2pdwkh5aAAA'
      )
    ).rejects.toThrow()
  })
})
