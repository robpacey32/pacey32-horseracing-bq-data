import { getApp, getApps, initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore } from "firebase/firestore";

const firebaseConfig = {
  apiKey: "AIzaSyAdXojWrR4Z1MZ2D9KYPb1g9XJ0MpKFlsc",
  authDomain: "horseracing-pacey32-github.firebaseapp.com",
  projectId: "horseracing-pacey32-github",
  storageBucket: "horseracing-pacey32-github.firebasestorage.app",
  messagingSenderId: "1096218037457",
  appId: "1:1096218037457:web:0c01bced082c912c6649f0",
};

export const firebaseApp = getApps().length
  ? getApp()
  : initializeApp(firebaseConfig);

export const auth = getAuth(firebaseApp);
export const db = getFirestore(firebaseApp);