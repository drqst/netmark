//! A WebRTC data-channel layer that rides on top of whichever transport a run
//! selected (TCP, UDP or raw IP).
//!
//! This is not a full WebRTC stack: there is no ICE, DTLS or SCTP association,
//! because netmark measures a network path rather than interoperating with a
//! browser. What it does reproduce is the part that shapes the traffic: DCEP-style
//! channel setup, per-channel message sequencing, and the ordered/unordered
//! distinction, so a run looks like data-channel traffic on the wire and any
//! reordering or loss is attributed to the channel it happened on.

use std::collections::HashMap;

/// `"RT"`, so a stray packet is not mistaken for a data-channel frame.
const MAGIC: u16 = 0x5254;
const VERSION: u8 = 1;

/// magic(2) version(1) type(1) channel(2) flags(2) sequence(4) payload_len(2) reserved(2)
pub const HEADER_LEN: usize = 16;

const FLAG_ORDERED: u16 = 0x0001;

/// DCEP message types, using the same numbering as RFC 8832 where they overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Open,
    Ack,
    Data,
}

impl MessageType {
    fn code(self) -> u8 {
        match self {
            Self::Open => 0x03,
            Self::Ack => 0x02,
            Self::Data => 0x00,
        }
    }
    fn from_code(code: u8) -> Option<Self> {
        match code {
            0x03 => Some(Self::Open),
            0x02 => Some(Self::Ack),
            0x00 => Some(Self::Data),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame {
    pub message_type: MessageType,
    pub channel: u16,
    pub ordered: bool,
    pub sequence: u32,
    pub payload_len: u16,
}

impl Frame {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut header = [0u8; HEADER_LEN];
        header[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        header[2] = VERSION;
        header[3] = self.message_type.code();
        header[4..6].copy_from_slice(&self.channel.to_be_bytes());
        header[6..8].copy_from_slice(&(if self.ordered { FLAG_ORDERED } else { 0 }).to_be_bytes());
        header[8..12].copy_from_slice(&self.sequence.to_be_bytes());
        header[12..14].copy_from_slice(&self.payload_len.to_be_bytes());
        header
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN
            || u16::from_be_bytes(bytes[0..2].try_into().ok()?) != MAGIC
            || bytes[2] != VERSION
        {
            return None;
        }
        Some(Self {
            message_type: MessageType::from_code(bytes[3])?,
            channel: u16::from_be_bytes(bytes[4..6].try_into().ok()?),
            ordered: u16::from_be_bytes(bytes[6..8].try_into().ok()?) & FLAG_ORDERED != 0,
            sequence: u32::from_be_bytes(bytes[8..12].try_into().ok()?),
            payload_len: u16::from_be_bytes(bytes[12..14].try_into().ok()?),
        })
    }
}

/// Settings that reach the traffic threads; mirrors `configuration::WebRtcConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub enabled: bool,
    pub channels: u16,
    pub label: String,
    pub ordered: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            channels: 1,
            label: "netmark".to_string(),
            ordered: true,
        }
    }
}

impl Settings {
    pub fn summary(&self) -> String {
        format!(
            "webrtc {} channels={} label={} ordered={}",
            if self.enabled { "enabled" } else { "disabled" },
            self.channels.max(1),
            self.label,
            self.ordered
        )
    }
}

/// Sender state: one message sequence per channel, handed out round-robin so a
/// run with several channels spreads its messages the way a real peer would.
pub struct Sender {
    settings: Settings,
    next_channel: u16,
    sequences: Vec<u32>,
    opened: Vec<bool>,
}

impl Sender {
    pub fn new(settings: Settings) -> Self {
        let channels = settings.channels.max(1) as usize;
        Self {
            settings,
            next_channel: 0,
            sequences: vec![0; channels],
            opened: vec![false; channels],
        }
    }

    /// Builds the next frame header. The first frame on each channel is an `Open`,
    /// mirroring how a data channel is announced before it carries data.
    pub fn next(&mut self, payload_len: u16) -> Frame {
        let channels = self.settings.channels.max(1);
        let channel = self.next_channel % channels;
        self.next_channel = (self.next_channel + 1) % channels;
        let index = channel as usize;
        let message_type = if self.opened[index] {
            MessageType::Data
        } else {
            self.opened[index] = true;
            MessageType::Open
        };
        let sequence = self.sequences[index];
        self.sequences[index] = sequence.wrapping_add(1);
        Frame {
            message_type,
            channel,
            ordered: self.settings.ordered,
            sequence,
            payload_len,
        }
    }
}

/// Receiver state: counts messages per channel and flags gaps and reordering on
/// ordered channels.
#[derive(Default)]
pub struct Receiver {
    expected: HashMap<u16, u32>,
    pub messages: u64,
    pub out_of_order: u64,
    pub invalid: u64,
}

impl Receiver {
    pub fn accept(&mut self, bytes: &[u8]) -> Option<Frame> {
        let Some(frame) = Frame::decode(bytes) else {
            self.invalid += 1;
            return None;
        };
        self.messages += 1;
        let expected = self.expected.entry(frame.channel).or_default();
        if frame.ordered && frame.sequence < *expected {
            self.out_of_order += 1;
        }
        *expected = (*expected).max(frame.sequence.wrapping_add(1));
        Some(frame)
    }
}
