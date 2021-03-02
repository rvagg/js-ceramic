import CID from "cids"

function isCID(data: string | CID) {
  try {
    new CID(data.toString())
    return true
  } catch {
    return false
  }
}

export function serialize (data: any): any {
  if (Array.isArray(data)) {
    const serialized = []
    for (const item of data) {
      serialized.push(serialize(item))
    }
    return serialized
  }
  if (!isCID(data) && typeof data === "object") {
    const serialized: Record<string, any> = {}
    for (const prop in data) {
      serialized[prop] = serialize(data[prop])
    }
    return serialized
  }
  if (isCID(data)) {
    return data.toString()
  }
  return data
}
