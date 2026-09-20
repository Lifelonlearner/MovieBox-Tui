pub struct PresetPlaylist {
    pub name: &'static str,
    pub description: &'static str,
    pub url: &'static str,
}

pub const INDIAN_PRESETS: &[PresetPlaylist] = &[
    PresetPlaylist {
        name: "India (All Channels)",
        description: "National and Regional Indian channels",
        url: "https://iptv-org.github.io/iptv/countries/in.m3u",
    },
    PresetPlaylist {
        name: "Hindi Channels",
        description: "Hindi News, Entertainment, Music",
        url: "https://iptv-org.github.io/iptv/languages/hin.m3u",
    },
];

pub fn default_preset_urls() -> Vec<String> {
    vec![
        "https://iptv-org.github.io/iptv/countries/in.m3u".to_string(),
    ]
}
