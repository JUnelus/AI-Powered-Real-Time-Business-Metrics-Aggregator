resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: 'YourSynapseWorkspace'
  location: 'eastus'
  properties: {
    defaultDataLakeStorage: {
      accountUrl: 'https://yourdatalake.dfs.core.windows.net/'
      filesystem: 'datalake'
    }
  }
}

resource synapsePipeline 'Microsoft.Synapse/workspaces/pipelines@2021-06-01' = {
  name: 'YourSynapsePipeline'
  parent: synapseWorkspace
  properties: {
    activities: [
      {
        name: 'DataIngestion'
        type: 'CopyActivity'
        inputs: ['business_metrics']
        outputs: ['transformed_business_metrics']
      }
    ]
  }
}
