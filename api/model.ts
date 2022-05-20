
import mongoose from 'mongoose';

// @ts-ignore
import { config } from './config';

async function connectDB() {
    console.log("connecting to mongose");
    await mongoose.connect(config.mongo);
}

const Log = mongoose.model('log', new mongoose.Schema({
    age: Number,
    gender: String,
    profileKey: String, //zip, county, etc..

    headers: mongoose.Schema.Types.Mixed, //http header
    date: { type: Date, default: Date.now },
}));

export {
    //db: mongoose.connection,
    Log,
    connectDB,
};


