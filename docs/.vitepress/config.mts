import { defineConfig } from "vitepress";

export default defineConfig({
	title: "Beignet",
	description: "A durable runtime for agent turns that outlive their clients.",
	cleanUrls: true,
	themeConfig: {
		nav: [
			{ text: "Guide", link: "/quickstart" },
			{ text: "v0.1", link: "/V0.1" },
		],
		sidebar: [
			{
				text: "Beignet v0.1",
				items: [
					{ text: "Overview", link: "/" },
					{ text: "Quickstart", link: "/quickstart" },
					{ text: "Architecture", link: "/DESIGN" },
					{ text: "Wire contract", link: "/CONTRACT" },
					{ text: "Operations", link: "/operations" },
					{ text: "Security", link: "/security" },
					{ text: "Scope and guarantees", link: "/V0.1" },
					{ text: "Release notes", link: "/release-notes" },
				],
			},
		],
		socialLinks: [
			{ icon: "github", link: "https://github.com/Dreeseaw/beignet" },
		],
		search: { provider: "local" },
	},
});
