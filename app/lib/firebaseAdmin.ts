import {
  applicationDefault,
  getApps,
  initializeApp,
} from "firebase-admin/app";

import { getAuth } from "firebase-admin/auth";

import { getFirestore } from "firebase-admin/firestore";

const adminApp =
  getApps()[0] ??
  initializeApp({
    credential: applicationDefault(),
    projectId: "horseracing-pacey32-github",
  });

export const adminAuth = getAuth(adminApp);

export const adminDb = getFirestore(adminApp);
