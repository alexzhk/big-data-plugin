/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.formats.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;

import java.net.URI;
import java.net.URISyntaxException;

public abstract class NamedClusterResolver {

  private static String extractScheme( String fileUri ) {
    String scheme = null;
    try {
      scheme = new URI( fileUri ).getScheme();
      return scheme;
    } catch ( URISyntaxException e ) {
      e.printStackTrace();
    }
    return scheme;
  }

  private static String extractHostName( String fileUri ) {
    String hostName = null;
    try {
      hostName = new URI( fileUri ).getHost();
      return hostName;
    } catch ( URISyntaxException e ) {
      e.printStackTrace();
    }
    return hostName;
  }

  public static NamedCluster resolveNamedCluster( NamedClusterService namedClusterService,
                                                  MetastoreLocatorOsgi metaStoreService, String fileUri ) {
    NamedCluster namedCluster;
    String scheme = extractScheme( fileUri );
    String hostName = extractHostName( fileUri );
    if ( scheme.equals( "hc" ) ) {
      namedCluster = namedClusterService.getNamedClusterByName( hostName, metaStoreService.getMetastore() );
    } else {
      namedCluster = namedClusterService.getNamedClusterByHost( hostName, metaStoreService.getMetastore() );
    }
    if ( namedCluster == null ) {
      namedCluster = namedClusterService.getClusterTemplate();
    }
    return namedCluster;
  }
}
