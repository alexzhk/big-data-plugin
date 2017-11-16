/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.big.data.impl.vfs.hdfs.nc.NamedClusterConfigBuilder;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.xml.XmlMetaStoreElementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

public class HDFSFileProvider extends AbstractOriginatingFileProvider {

  protected static Logger logger = LoggerFactory.getLogger( HDFSFileProvider.class );
  private MetastoreLocatorOsgi metaStoreService;
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "hdfs";
  public static final String MAPRFS = "maprfs";
  /**
   * User Information.
   */
  public static final String ATTR_USER_INFO = "UI";
  /**
   * Authentication types.
   */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
    new UserAuthenticationData.Type[] { UserAuthenticationData.USERNAME,
      UserAuthenticationData.PASSWORD };
  /**
   * The provider's capabilities.
   */
  public static final Collection<Capability> capabilities =
    Collections.unmodifiableCollection( Arrays.asList( new Capability[] { Capability.CREATE, Capability.DELETE,
      Capability.RENAME, Capability.GET_TYPE, Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI,
      Capability.WRITE_CONTENT, Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE,
      Capability.RANDOM_ACCESS_READ } ) );
  protected final HadoopFileSystemLocator hadoopFileSystemLocator;
  protected final NamedClusterService namedClusterService;

  @Deprecated
  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                           NamedClusterService namedClusterService, MetastoreLocatorOsgi metaStore ) throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(), metaStore );
  }

  @Deprecated
  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator, NamedClusterService namedClusterService,
                           DefaultFileSystemManager fileSystemManager, MetastoreLocatorOsgi metaStore ) throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService, fileSystemManager, HDFSFileNameParser.getInstance(),
      new String[] { SCHEME, MAPRFS }, metaStore );
  }

  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator, NamedClusterService namedClusterService,
                           FileNameParser fileNameParser, String schema, MetastoreLocatorOsgi metaStore ) throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
      fileNameParser, new String[] { schema }, metaStore );
  }

  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator, NamedClusterService namedClusterService,
                           DefaultFileSystemManager fileSystemManager, FileNameParser fileNameParser, String[] schemes,
                           MetastoreLocatorOsgi metaStore)
    throws FileSystemException {
    super();
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.namedClusterService = namedClusterService;
    this.metaStoreService = metaStore;
    setFileNameParser( fileNameParser );
    fileSystemManager.addProvider( schemes, this );
  }

  @Override protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    String hostName = genericFileName.getHostName();
    int port = genericFileName.getPort();
    NamedCluster namedCluster = getNamedClusterByHost( hostName, port, name );
    try {
      return new HDFSFileSystem( name, fileSystemOptions, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster,
        URI.create( name.getURI() == null ? "" : name.getURI() ) ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }

  @Override public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return NamedClusterConfigBuilder.getInstance( metaStoreService, namedClusterService );
  }

  private NamedCluster getNamedClusterByHost( String hostName, int port, final FileName name ) {
    for (NamedCluster namedCluster : getAllNamedClusters()) {
      if (namedCluster.getHdfsHost().equals(hostName)) {
        return namedCluster;
      }
    }

    NamedCluster namedCluster = namedClusterService.getClusterTemplate();
    namedCluster.setHdfsHost( hostName );
    if ( port > 0 ) {
      namedCluster.setHdfsPort( String.valueOf( port ) );
    } else {
      namedCluster.setHdfsPort( "" );
    }
    namedCluster.setMapr( MAPRFS.equals( name.getScheme() ) );

    return namedCluster;
  }

  private List<NamedCluster> getAllNamedClusters(  ) {
    try {
      return namedClusterService.list(metaStoreService.getMetastore());
    } catch (MetaStoreException e) {
      e.printStackTrace();
    }

    return Collections.emptyList();
  }
}
