const {Bigtable} = require('@google-cloud/bigtable');

const bigtable = new Bigtable();

const init = async(instanceFromID , instanceToID , tableNameFrom , tableNameTo, account)=>{
    const instanceFrom = bigtable.instance(instanceFromID);
    if(!(await instanceFrom.exists())){
        throw new Error("Couldn't find source instance");
    }

    const instanceTo = bigtable.instance(instanceToID);
    if(!(await instanceTo.exists())){
        throw new Error("Couldn't find destination instance");
    }
    // instance from which we fetch data must have given table
    const tableFrom = instanceFrom.table(tableNameFrom);
    if(!await tableFrom.exists()){
        throw new Error("Couldn't find table in existing instance");
    }

    const tableTo = instanceTo.table(tableNameTo);
    if(!await tableTo.exists()){
        console.log("Couldn't find table in destination instance... Creating new table");
        try{
            await tableTo.create();
        } catch(e){
            throw new Error("ERROR! Problem creating table" , e);
        }
    }

    // now its certain that both the tables exist
    await addMissingColumnFamily(tableFrom, tableTo);

    // now we can just add the data from source table to destination table
    await migrateTable(tableFrom , tableTo , account);

}

async function addMissingColumnFamily(tableFrom , tableTo){
    const [tableToMetaData] = await tableTo.getMetadata();
    const [tableFromMetaData] = await tableFrom.getMetadata();

    const columnFamiliesFrom = tableFromMetaData.columnFamilies;
    const columnFamiliesTo = tableToMetaData.columnFamilies;

    for(let columnFamily in columnFamiliesFrom){
        if(columnFamily in columnFamiliesTo){
            continue;
        } 
        else{
            // create new column family with same rules 
            // console.log(columnFamily , columnFamiliesFrom[columnFamily].gcRule);
            if(columnFamiliesFrom[columnFamily].gcRule!=null)
                await tableTo.createFamily(columnFamily , columnFamiliesFrom[columnFamily].gcRule);
            else
                await tableTo.createFamily(columnFamily);
        }
    }
}

async function migrateTable(tableFrom , tableTo , account){
    console.log("Migrating");
    const BATCH = 10000;
    // regexAcc = regex se account ko rowkey banana hai 
    const rowKey = account.toString()+"_";
    // fetch data from tableFrom ,put in array . once batch size reached flush in tableTo
    let batch = [];

    await tableFrom.createReadStream({
        prefix:rowKey,
    })
    .on('error' , (err)=>{
        console.error("Error in reading data" , err);
    })
    .on('data' , async (row)=>{
        // console.log(printRow(row.id , row.data));
        batch.push(row);

        if(batch.length >= BATCH){
            await writeToTable(tableTo , batch);
            batch = [];
        }
    })
    .on('end' , async ()=>{
        if(batch.length > 0){
            await writeToTable(tableTo , batch);
        }

        console.log("Table has been successfully copied");
    })    
} 

async function writeToTable(tableTo, batch) {
    console.log("writeToTable");
    try {
        const mutations = batch.map(row => {
            const rowMutations = Object.entries(row.data).flatMap(([family, qualifiers]) =>
                Object.entries(qualifiers).flatMap(([qualifier, cells]) =>
                    cells.map(cell => ({
                        method: 'insert',
                        key: row.id,
                        data: {
                            [family]: {
                                [qualifier]: {
                                    value: cell.value,
                                    timestamp: cell.timestamp, //new Date(),
                                }
                            }
                        }
                    }))
                )
            );
            return rowMutations;
        }).flat();

        if(mutations.length > 0){
            await tableTo.mutate(mutations);
        }
    } catch (e) {
        console.error("Error while writing to table", e);
    }
}

const _instanceFromID = 'instance1';
const _instanceToID = 'instance2';
const _tableNameFrom = 'my-table';
const _tableNameTo = 'copied-table';
const _account = '1234'; // prefix

init(_instanceFromID , _instanceToID , _tableNameFrom , _tableNameTo , _account);