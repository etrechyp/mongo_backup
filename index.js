const mongoose = require("mongoose");
const moment = require("moment");
const fs = require("fs");
const { S3 } = require("aws-sdk");
const archiver = require("archiver");
const cron = require("node-cron");
require("dotenv").config();

const uri = process.env.MONGO_URI;

mongoose.connect(uri, { useNewUrlParser: true, useUnifiedTopology: true });

const db = mongoose.connection;

db.on("error", console.error.bind(console, "Error de conexión a MongoDB:"));
db.once("open", async () => {
    console.log("Conectado a MongoDB");

    cron.schedule("0 2 * * 0", async () => {
        try {
            const backupRootFolder = 'backups';
            if (!fs.existsSync(backupRootFolder)) {
                fs.mkdirSync(backupRootFolder);
            }

            const backupFolder = `${backupRootFolder}/${moment().format("YYYY-MM-DD")}`;
            if (!fs.existsSync(backupFolder)) {
                fs.mkdirSync(backupFolder, { recursive: true });
            }

            const timestamp = moment().format("YYYY-MM-DD_HH-mm-ss");
            const backupFileName = `${timestamp}_backup`;

            const collections = await mongoose.connection.db.listCollections().toArray();
            const promises = collections.map(async (collection) => {
                const data = await mongoose.connection.db.collection(collection.name).find().toArray();
                fs.writeFileSync(`${backupFolder}/${backupFileName}_${collection.name}.json`, JSON.stringify(data, null, 2));
            });

            await Promise.all(promises);

            const zipPath = `${backupFolder}/${backupFileName}.zip`;
            const output = fs.createWriteStream(zipPath);
            const archive = archiver("zip", { zlib: { level: 9 } });

            archive.pipe(output);
            archive.directory(backupFolder, false);
            await archive.finalize();

            console.log("Copia de seguridad local comprimida realizada con éxito.");
            console.log("Ruta del backup local comprimido:", zipPath);

            if (fs.existsSync(zipPath)) {
                const s3 = new S3({
                    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
                    region: process.env.AWS_REGION,
                });

                const s3Params = {
                    Bucket: process.env.AWS_BUCKET_NAME,
                    Key: `backups/${backupFileName}.zip`,
                    Body: fs.createReadStream(zipPath),
                };

                await s3.upload(s3Params).promise();

                console.log("Copia de seguridad en S3 realizada con éxito.");
                console.log("Ruta del backup en S3:", s3Params.Key);

                fs.readdirSync(backupFolder).forEach((file) => {
                    const filePath = `${backupFolder}/${file}`;
                    fs.unlinkSync(filePath);
                    console.log(`Archivo local eliminado: ${filePath}`);
                });

                fs.rmdirSync(backupFolder);
                console.log(`Carpeta local eliminada: ${backupFolder}`);
            } else {
                console.error("Error: No se pudo encontrar el archivo ZIP.");
            }
        } catch (error) {
            console.error("Error al realizar la copia de seguridad:", error);
        }
    });
});
