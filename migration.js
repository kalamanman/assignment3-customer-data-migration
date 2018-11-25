const mongodb =require('mongodb')
const customers=require('./m3-customer-data.json')
const addresses= require('./m3-customer-address-data.json')
const url='mongodb://localhost:27017'
const dbName='customersdb'
collectionName='customer'
async=require('async')
var tasks=[]
const limit =parseInt(process.argv[2],10) || 100
mongodb.MongoClient.connect(url,(error,client)=>{
    if(error) return process.exit(1)
    console.log('connected to the database')
    const db=client.db(dbName)
customers.forEach((customer,index)=>{
    customers[index]=Object.assign(customer,addresses[index])
    if(index%limit==0){
      const start=index
      const end=(start+limit>customers.length)?customers.length-1:index+limit
      tasks.push((task)=>{
          console.log(`Inserting customers from ${start}  to  ${end}`)
          db.collection(collectionName).insertMany(customers.slice(start,end),(error,result)=>{
              task(error,result)
          })
      })

    }
})    
    async.parallel(tasks,(error,result)=>{

        if(error) console.error('error')    
    })
    console.log(`Database migration has completed successfully\n Now exiting the program status code==0`)
   return  process.exit(0)
})