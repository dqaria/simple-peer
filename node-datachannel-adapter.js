/*! node-datachannel adapter for simple-peer */
const EventEmitter = require('events')

/**
 * Adapter to make node-datachannel compatible with WebRTC API
 */

class RTCPeerConnectionAdapter extends EventEmitter {
  constructor (nodeDataChannel, config) {
    super()
    this._ndc = nodeDataChannel
    this._config = config

    // Convert iceServers format
    const iceServers = (config.iceServers || []).map(server => {
      if (Array.isArray(server.urls)) {
        return server.urls
      }
      return server.urls || []
    }).flat()

    this._pc = new nodeDataChannel.PeerConnection('peer', {
      iceServers
    })

    // Set up event forwarding
    this._pc.onLocalDescription((sdp, type) => {
      this.localDescription = { sdp, type }
      // Trigger icecandidate event with null to signal gathering complete
      if (this.onicecandidate) {
        this.onicecandidate({ candidate: null })
      }
    })

    this._pc.onLocalCandidate((candidate, mid) => {
      if (this.onicecandidate) {
        this.onicecandidate({
          candidate: {
            candidate,
            sdpMid: mid,
            sdpMLineIndex: 0
          }
        })
      }
    })

    this._pc.onStateChange((state) => {
      this.iceConnectionState = state.toLowerCase()
      this.connectionState = state.toLowerCase()
      if (this.oniceconnectionstatechange) {
        this.oniceconnectionstatechange()
      }
      if (this.onconnectionstatechange) {
        this.onconnectionstatechange()
      }
    })

    this._pc.onGatheringStateChange((state) => {
      this.iceGatheringState = state.toLowerCase()
      if (this.onicegatheringstatechange) {
        this.onicegatheringstatechange()
      }
    })

    this._pc.onDataChannel((dc) => {
      if (this.ondatachannel) {
        const adapter = new RTCDataChannelAdapter(dc)
        this.ondatachannel({ channel: adapter })
      }
    })

    // Initialize states
    this.localDescription = null
    this.remoteDescription = null
    this.signalingState = 'stable'
    this.iceConnectionState = 'new'
    this.connectionState = 'new'
    this.iceGatheringState = 'new'

    // Event handlers (will be set by user code)
    this.onicecandidate = null
    this.ondatachannel = null
    this.oniceconnectionstatechange = null
    this.onconnectionstatechange = null
    this.onicegatheringstatechange = null
    this.onsignalingstatechange = null
  }

  createDataChannel (label, options = {}) {
    const dc = this._pc.createDataChannel(label, options)
    return new RTCDataChannelAdapter(dc)
  }

  async createOffer (options) {
    // node-datachannel doesn't have explicit createOffer
    // It automatically generates offer when needed
    return new Promise((resolve) => {
      // Wait for local description
      const checkLocalDesc = () => {
        if (this.localDescription) {
          resolve(this.localDescription)
        } else {
          setTimeout(checkLocalDesc, 10)
        }
      }
      checkLocalDesc()
    })
  }

  async createAnswer (options) {
    // Similar to createOffer
    return new Promise((resolve) => {
      const checkLocalDesc = () => {
        if (this.localDescription) {
          resolve(this.localDescription)
        } else {
          setTimeout(checkLocalDesc, 10)
        }
      }
      checkLocalDesc()
    })
  }

  async setLocalDescription (desc) {
    this.localDescription = desc
    this._updateSignalingState()
    return Promise.resolve()
  }

  async setRemoteDescription (desc) {
    this._pc.setRemoteDescription(desc.sdp, desc.type)
    this.remoteDescription = desc
    this._updateSignalingState()
    return Promise.resolve()
  }

  async addIceCandidate (candidate) {
    if (candidate && candidate.candidate) {
      this._pc.addRemoteCandidate(candidate.candidate, candidate.sdpMid || '0')
    }
    return Promise.resolve()
  }

  _updateSignalingState () {
    const oldState = this.signalingState

    if (this.localDescription && this.remoteDescription) {
      this.signalingState = 'stable'
    } else if (this.localDescription) {
      this.signalingState = this.localDescription.type === 'offer' ? 'have-local-offer' : 'have-local-answer'
    } else if (this.remoteDescription) {
      this.signalingState = this.remoteDescription.type === 'offer' ? 'have-remote-offer' : 'have-remote-answer'
    }

    if (oldState !== this.signalingState && this.onsignalingstatechange) {
      this.onsignalingstatechange()
    }
  }

  close () {
    if (this._pc) {
      this._pc.close()
    }
  }

  getStats (callback) {
    // node-datachannel doesn't provide detailed stats
    // Return empty array for compatibility
    if (callback) {
      setImmediate(() => callback(null, []))
    }
    return Promise.resolve([])
  }
}

class RTCDataChannelAdapter extends EventEmitter {
  constructor (dc) {
    super()
    this._dc = dc

    // Forward events
    this._dc.onOpen(() => {
      this.readyState = 'open'
      if (this.onopen) this.onopen()
    })

    this._dc.onClosed(() => {
      this.readyState = 'closed'
      if (this.onclose) this.onclose()
    })

    this._dc.onError((err) => {
      if (this.onerror) {
        this.onerror({ error: new Error(err) })
      }
    })

    this._dc.onMessage((data) => {
      if (this.onmessage) {
        // Convert data to appropriate format
        let messageData
        if (typeof data === 'string') {
          messageData = data
        } else if (data instanceof ArrayBuffer) {
          messageData = data
        } else if (Buffer.isBuffer(data)) {
          messageData = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
        } else {
          messageData = data
        }
        this.onmessage({ data: messageData })
      }
    })

    this._dc.onBufferedAmountLow(() => {
      if (this.onbufferedamountlow) {
        this.onbufferedamountlow()
      }
    })

    // Initialize properties
    this.label = dc.getLabel ? dc.getLabel() : ''
    this.readyState = 'connecting'
    this.binaryType = 'arraybuffer'
    this.bufferedAmountLowThreshold = 0

    // Event handlers
    this.onopen = null
    this.onclose = null
    this.onerror = null
    this.onmessage = null
    this.onbufferedamountlow = null
  }

  get bufferedAmount () {
    return this._dc.bufferedAmount ? this._dc.bufferedAmount() : 0
  }

  send (data) {
    if (typeof data === 'string') {
      this._dc.sendMessage(data)
    } else if (data instanceof ArrayBuffer) {
      this._dc.sendMessageBinary(Buffer.from(data))
    } else if (Buffer.isBuffer(data)) {
      this._dc.sendMessageBinary(data)
    } else if (ArrayBuffer.isView(data)) {
      this._dc.sendMessageBinary(Buffer.from(data.buffer, data.byteOffset, data.byteLength))
    } else {
      throw new Error('Unsupported data type')
    }
  }

  close () {
    if (this._dc) {
      this._dc.close()
    }
  }
}

class RTCSessionDescriptionAdapter {
  constructor (init) {
    this.type = init.type
    this.sdp = init.sdp
  }
}

class RTCIceCandidateAdapter {
  constructor (init) {
    this.candidate = init.candidate
    this.sdpMid = init.sdpMid
    this.sdpMLineIndex = init.sdpMLineIndex
  }
}

function getNodeDataChannelRTC () {
  try {
    const nodeDataChannel = require('node-datachannel')

    return {
      RTCPeerConnection: function (config) {
        return new RTCPeerConnectionAdapter(nodeDataChannel, config)
      },
      RTCSessionDescription: RTCSessionDescriptionAdapter,
      RTCIceCandidate: RTCIceCandidateAdapter
    }
  } catch (err) {
    return null
  }
}

module.exports = getNodeDataChannelRTC
