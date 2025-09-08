declare namespace NodeJS {
	interface ProcessEnv {
		NODE_ENV: "development" | "production" | "test";
		// PORT?: string;

		// 👇 Add your custom env variables here
		SPOTIFY_CLIENT_ID: string;
		SPOTIFY_CLIENT_SECRET: string;
		DATABASE_URL: string;
	}
}
