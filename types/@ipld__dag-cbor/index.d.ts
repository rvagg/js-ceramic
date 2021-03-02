declare module '@ipld/dag-cbor' {
  export const name: string
  export const code: number
  export function encode(input: any): Uint8Array
  export function decode(input: Uint8Array): any
}
